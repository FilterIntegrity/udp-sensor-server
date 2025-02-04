import socket
import logging
from datetime import datetime
import sqlite3
from pathlib import Path
import csv
import json
from collections import defaultdict

class IoTUDPServer:
    def __init__(self, host='0.0.0.0', port=56790, buffer_size=1024):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler('iot_server.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Buffer to store incomplete packet sets
        self.packet_buffer = defaultdict(dict)
        
        # Initialize database
        self.db_path = Path('sensor_data.db')
        self.init_database()
        
        # Ensure output directory exists
        self.output_dir = Path('output')
        self.output_dir.mkdir(exist_ok=True)

    def init_database(self):
        """Initialize SQLite database for storing sensor data."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_readings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        serial_number TEXT,
                        channel_data TEXT,
                        raw_packets TEXT
                    )
                ''')
                conn.commit()
            self.logger.info(f"Database initialized at {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization error: {e}")
            raise

    def parse_packet(self, packet_bytes):
        """Parse a single UDP packet."""
        try:
            packet_str = packet_bytes.decode('utf-8')
            
            # Extract basic components
            serial = packet_str.split('<')[0]
            content = packet_str.split('<')[1].split('>')[0]
            checksum = packet_str.split('>')[-1]
            
            # Parse channel data
            channel_data = {}
            if 'sendVal' in content:
                channels_part = content.replace('sendVal', '').strip()
                if channels_part:  # If there's data after sendVal
                    for channel in channels_part.split(';'):
                        if '=' in channel:
                            channel_num, value = channel.strip().split('=')
                            try:
                                # Handle NaN values
                                if value.strip() == 'NaN':
                                    channel_data[channel_num] = None
                                else:
                                    channel_data[channel_num] = float(value)
                            except ValueError:
                                self.logger.warning(f"Could not parse value for channel {channel_num}: {value}")
                
            return {
                'serial': serial,
                'channel_data': channel_data,
                'checksum': checksum,
                'raw_packet': packet_str,
                'is_end_marker': bool(not channels_part.strip())
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing packet: {e}")
            return None

    def save_to_database(self, serial, channel_data, raw_packets):
        """Save complete dataset to SQLite database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO sensor_readings (serial_number, channel_data, raw_packets)
                    VALUES (?, ?, ?)
                ''', (
                    serial,
                    json.dumps(channel_data),
                    json.dumps(raw_packets)
                ))
                conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database error while saving data: {e}")

    def write_to_csv(self, serial, channel_data):
        """Write the complete dataset to a CSV file with timestamp and horizontal channel layout."""
        timestamp = datetime.now()
        date_str = timestamp.strftime("%Y%m%d")
        filename = self.output_dir / f"sensor_data_{serial}_{date_str}.csv"
        
        try:
            # Determine if file exists to handle headers
            file_exists = filename.exists()
            
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                # Create header row if file is new
                if not file_exists:
                    # Get all channel numbers sorted numerically
                    channels = sorted(int(k) for k in channel_data.keys())
                    # Create headers: timestamp + channel numbers
                    headers = ['Timestamp'] + [f'Channel_{ch}' for ch in channels]
                    writer.writerow(headers)
                
                # Create data row
                # First column is timestamp
                row_data = [timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]]
                
                # Add channel values in order
                for ch in sorted(int(k) for k in channel_data.keys()):
                    value = channel_data[str(ch)]
                    row_data.append(value if value is not None else 'NaN')
                
                writer.writerow(row_data)
            
            self.logger.info(f"Data appended to CSV file: {filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing CSV file: {e}")

    def process_complete_dataset(self, serial):
        """Process a complete set of packets for a device."""
        try:
            # Combine all channel data
            combined_channel_data = {}
            raw_packets = []
            
            for packet_info in self.packet_buffer[serial].values():
                combined_channel_data.update(packet_info['channel_data'])
                raw_packets.append(packet_info['raw_packet'])
            
            # Save to database
            self.save_to_database(serial, combined_channel_data, raw_packets)
            
            # Write to CSV
            self.write_to_csv(serial, combined_channel_data)
            
            # Clear the buffer for this device
            del self.packet_buffer[serial]
            
        except Exception as e:
            self.logger.error(f"Error processing complete dataset: {e}")

    def start(self):
        """Start the UDP server and listen for incoming data."""
        self.logger.info(f"Starting UDP server on {self.host}:{self.port}")
        self.logger.info("Waiting for data... Press Ctrl+C to stop.")
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.host, self.port))
        
        try:
            while True:
                data, addr = sock.recvfrom(self.buffer_size)
                parsed = self.parse_packet(data)
                
                if parsed:
                    serial = parsed['serial']
                    
                    if parsed['is_end_marker']:
                        # End marker received, process the complete dataset
                        if serial in self.packet_buffer:
                            self.process_complete_dataset(serial)
                    else:
                        # Store the packet data
                        self.packet_buffer[serial][len(self.packet_buffer[serial])] = parsed
                
        except KeyboardInterrupt:
            self.logger.info("\nServer shutdown requested")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            sock.close()
            self.logger.info("Server shutdown complete")

if __name__ == "__main__":
    server = IoTUDPServer()
    server.start()
