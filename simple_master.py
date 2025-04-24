import time
import datetime
import logging

import click
import hart_protocol
import serial
from paho.mqtt import client as mqtt_client

from times import BAUDRATE, STO

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s]: %(message)s')


def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        logger.debug('Connected to MQTT Broker!')
    else:
        logger.debug(f"Failed to connect, return code: {rc}")


class Client:
    def __init__(self, username, password, broker, port):
        self.client = mqtt_client.Client(callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)
        self.client.username_pw_set(username, password)
        self.client.connect(broker, port)
        self.client.on_connect = on_connect

    def __enter__(self):
        self.client.loop_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.loop_stop()

    def publish(self, topic, data):
        result = self.client.publish(topic, data)
        status = result[0]
        if status == 0:
            print(f"{datetime.datetime.now().time()} Send `{data}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")


class SimpleMaster:
    def __init__(self, port):
        self.s = serial.Serial(port=port, baudrate=BAUDRATE)
        self.unpacker = hart_protocol.Unpacker(self.s, on_error='continue')
        self.manufacturer_id = None
        self.manufacturer_device_type = None
        self.device_id = None
        self.long_address = None

    def pack_short_frame(self, address, command_id, preamble_len=20, data=None):
        if type(address) == bytes:
            address = int.from_bytes(address, "big")
        if type(command_id) == int:
            command_id = command_id.to_bytes(1, "big")
        command = b"\xFF" * preamble_len  # preamble
        command += b"\x02"  # start charachter
        command += (128 | address).to_bytes(1, 'big')
        command += command_id
        if data is None:
            command += b"\x00"  # byte count
        else:
            command += len(data).to_bytes(1, "big")  # byte count
            command += data  # data
        command += hart_protocol.tools.calculate_checksum(command[preamble_len:])
        return command

    def run_command_0(self):
        self.s.write(self.pack_short_frame(0, 0))
        # s.write(hart_protocol.tools.pack_command(0, 0))
        logger.debug('Send command 0')
        time.sleep(STO)

        for msg in self.unpacker:
            logger.debug('Response from slave ' + str(msg))
            self.manufacturer_id = msg.manufacturer_id
            self.manufacturer_device_type = msg.manufacturer_device_type
            self.device_id = msg.device_id.to_bytes(3, 'big')

        if self.manufacturer_id is None or self.manufacturer_device_type is None or self.device_id is None:
            return False

        self.long_address = hart_protocol.tools.calculate_long_address(
            self.manufacturer_id,
            self.manufacturer_device_type,
            self.device_id
        )
        return True

    def run_command_1(self):
        logger.debug('Send command 1')
        self.s.write(hart_protocol.tools.pack_command(self.long_address, 1))
        time.sleep(STO)
        for msg in self.unpacker:
            logger.debug('Response from slave ' + str(msg))
        time.sleep(1)

    def run_command_3(self):
        logger.debug('Send command 3')
        self.s.write(hart_protocol.tools.pack_command(self.long_address, 3))
        time.sleep(STO)
        data = {'primary': None,
                'primary_units': None,
                'secondary': None,
                'secondary_units': None}
        for msg in self.unpacker:
            data['primary'] = msg.primary_variable
            # data['primary_units'] = msg.primary_variable_units
            data['secondary'] = msg.secondary_variable
            # data['secondary_units'] = msg.secondary_variable_units
            logger.debug('Response from slave ' + str(msg))
        data['secondary'] = data['secondary'] * 1_000_000
        time.sleep(1)
        return data


@click.command()
@click.option('--port', default='/dev/pts/1', help='Serial port address')
@click.option('--username', required=True, help='mqtt-broker username')
@click.option('--password', required=True, help='mqtt-broker user password', prompt=True, hide_input=True,
              confirmation_prompt=True)
@click.option('--broker', required=True, help='mqtt-broker address')
@click.option('--broker_port', required=True, type=int, help='mqtt-broker port')
def run(port, username, password, broker, broker_port):
    m = SimpleMaster(port)

    result = m.run_command_0()
    if not result:
        logger.debug('Failed to find and connect to the device.')
        exit()

    m.run_command_1()

    with Client(username, password, broker, broker_port) as cl:
        while True:
            data = m.run_command_3()
            cl.publish('press', data['primary'])
            cl.publish('temp', data['secondary'])


if __name__ == '__main__':
    run()
