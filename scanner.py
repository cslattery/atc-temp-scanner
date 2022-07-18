import asyncio
from bleak import BleakScanner

def detection_callback(device, advertisement_data):
    print(device.name, device.address, "RSSI:", device.rssi, advertisement_data)

async def main():
    scanner = BleakScanner(filters={"Transport":"le"})
    scanner.register_detection_callback(detection_callback)
    await scanner.start()
    await asyncio.sleep(5.0)
    await scanner.stop()
    for d in scanner.discovered_devices:
        print(d)
    return(scanner.discovered_devices)

dev = asyncio.run(main())