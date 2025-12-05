#!/usr/bin/env python3
"""
Powercut Schedule Scraper for Telegram Channels
Scrapes electricity outage schedules for ALL queues and outputs to JSON format
"""

import requests
from bs4 import BeautifulSoup
import re
import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import argparse


class PowercutScraper:

    def __init__(self, channel_url: str, region_id: str = "dnipro"):
        self.url = channel_url
        self.region_id = region_id
        
        self.months_uk = {
            "січня": "01", "лютого": "02", "березня": "03", "квітня": "04",
            "травня": "05", "червня": "06", "липня": "07", "серпня": "08",
            "вересня": "09", "жовтня": "10", "листопада": "11", "грудня": "12"
        }
        self.current_year = datetime.now().year

    def scrape_messages(self) -> Dict[str, Dict[float, List[str]]]:
        """Scrape messages from Telegram channel and extract schedules for ALL queues
        
        Returns:
            Dict with dates as keys, and dict of queue_number -> time_slots as values
            Example: {"05.12.2025": {1.1: ["07:00-10:00"], 3.1: ["14:00-18:00"]}}
        """
        print(f"Fetching data from {self.url}")
        
        # Fetch the webpage
        response = requests.get(self.url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch webpage: {response.status_code}")

        # Parse the webpage content
        soup = BeautifulSoup(response.content, 'html.parser')
        messages = soup.find_all('div', class_='tgme_widget_message_text')

        # Collect all schedules: date -> queue_number -> time_slots
        all_schedules = {}
        
        for message in messages:
            date = self.extract_date(message.get_text())
            # Extract schedules for ALL queues
            schedules_by_queue = self.extract_all_schedules(message.get_text())
            
            if date and schedules_by_queue:
                date_obj = datetime.strptime(date, "%d.%m.%Y")
                
                # Only process today or future dates
                today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                if date_obj >= today:
                    if date not in all_schedules:
                        all_schedules[date] = {}
                    
                    # Combine overlapping time slots for each queue
                    for queue_num, time_slots in schedules_by_queue.items():
                        combined_slots = self.combine_time_slots(date, time_slots)
                        if combined_slots:
                            all_schedules[date][queue_num] = combined_slots
                            print(f'Found schedules for {date}, Queue {queue_num}: {combined_slots}')
        
        return all_schedules

    def extract_all_schedules(self, message: str) -> Dict[float, List[str]]:
        """Extract schedule time slots for ALL queue numbers found in message
        
        Returns:
            Dict mapping queue_number to list of time slots
            Example: {1.1: ["07:00-10:00"], 3.1: ["14:00-18:00"]}
        """
        schedules_by_queue = {}
        
        # Pattern: "3.1 черга: з 07:00 до 10:00; з 14:00 до 18:00"
        pattern = re.compile(r'([\d.]+)\W*черга:\s*((?:\W+з\s\d{2}:\d{2}\sдо\s\d{2}:\d{2};?)+)')
        schedules_pattern = re.compile(r'з\s(\d{2}:\d{2})\sдо\s(\d{2}:\d{2})')
        
        for match in pattern.finditer(message):
            queue_info = match.group(1)
            queue_number = float(queue_info)
            
            if queue_number not in schedules_by_queue:
                schedules_by_queue[queue_number] = []
            
            schedules_info = schedules_pattern.finditer(match.group(2))
            for schedule in schedules_info:
                start_time = schedule.group(1)
                end_time = schedule.group(2)
                schedules_by_queue[queue_number].append(f"{start_time}-{end_time}")

        # Old style schedule matching: "з 07:00 до 10:00 відключається 1, 2, 3.1 черги"
        old_pattern = re.compile(
            r'(?:з\s(\d{2}:\d{2}\sдо\s\d{2}:\d{2});?)\sвідключа[ює]ться*([0-9\sта,.;:!?]*черг[аи])+')
        matches = old_pattern.findall(message)

        if matches:
            for match in matches:
                time_range = match[0].replace(" до ", "-")
                queue_info = match[1]
                
                # Extract all numbers (including decimals) from queue_info
                queue_numbers = re.findall(r'[\d.]+', queue_info)
                
                for queue_str in queue_numbers:
                    queue_number = float(queue_str)
                    if queue_number not in schedules_by_queue:
                        schedules_by_queue[queue_number] = []
                    schedules_by_queue[queue_number].append(time_range)

        return schedules_by_queue

    def extract_date(self, message: str) -> Optional[str]:
        """Extract date from message text"""
        date_pattern = re.compile(r'(\d{1,2})(?:-го)?\s([а-яА-Я]+)')
        match = date_pattern.search(message)
        if match:
            day = match.group(1)
            month_uk = match.group(2).lower()
            if month_uk in self.months_uk:
                month = self.months_uk[month_uk]
                return f"{day}.{month}.{self.current_year}"
        return None

    def combine_time_slots(self, date: str, time_slots: List[str]) -> List[str]:
        """Combine overlapping or contiguous time slots"""
        if not time_slots:
            return []

        # Parse and sort the time slots
        slots = []
        for slot in time_slots:
            start_time, end_time = slot.split('-')
            if end_time == "24:00":
                end_time = "23:59"
            start = datetime.strptime(f"{date} {start_time}", "%d.%m.%Y %H:%M")
            end = datetime.strptime(f"{date} {end_time}", "%d.%m.%Y %H:%M")
            slots.append((start, end))

        slots.sort()

        # Merge overlapping or contiguous time slots
        merged_slots = []
        current_start, current_end = slots[0]

        for start, end in slots[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged_slots.append((current_start, current_end))
                current_start, current_end = start, end

        merged_slots.append((current_start, current_end))

        # Convert merged time slots back to strings
        combined_slots = [f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}" for start, end in merged_slots]
        return combined_slots

    def generate_json(self, schedules: Dict[str, Dict[float, List[str]]], existing_data: Optional[dict] = None) -> dict:
        """Generate JSON data following dnipro.json schema for ALL queues
        
        Args:
            schedules: Dict with dates as keys, and dict of queue_number -> time_slots as values
        """
        
        # Use existing data or create new structure
        if existing_data:
            data = existing_data
        else:
            data = self.create_json_structure()
        
        # Update lastUpdated timestamp
        data["lastUpdated"] = datetime.utcnow().isoformat() + "Z"
        
        # Get all unique queue numbers from all dates
        all_queues = set()
        for date_schedules in schedules.values():
            all_queues.update(date_schedules.keys())
        
        print(f"Processing schedules for queues: {sorted(all_queues)}")
        
        # Process each date and its schedules
        for date_str, queues_schedules in schedules.items():
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            
            # Convert to Unix timestamp (midnight of that day)
            timestamp = int(date_obj.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            timestamp_key = str(timestamp)
            
            # Initialize date entry if it doesn't exist
            if timestamp_key not in data["fact"]["data"]:
                data["fact"]["data"][timestamp_key] = {}
            
            # Process each queue for this date
            for queue_number, time_slots in queues_schedules.items():
                queue_key = self.get_queue_key(queue_number)
                
                # Initialize queue entry if it doesn't exist
                if queue_key not in data["fact"]["data"][timestamp_key]:
                    data["fact"]["data"][timestamp_key][queue_key] = self.create_default_hours()
                
                # Update hours based on outage schedules
                hours_data = self.create_hours_from_schedules(time_slots)
                data["fact"]["data"][timestamp_key][queue_key] = hours_data
        
        # Update fact metadata
        data["fact"]["update"] = datetime.now().strftime("%d.%m.%Y %H:%M")
        
        # Set today's timestamp
        today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        data["fact"]["today"] = int(today_midnight.timestamp())
        
        # Update lastUpdateStatus
        data["lastUpdateStatus"]["at"] = datetime.utcnow().isoformat() + "Z"
        
        return data

    def create_json_structure(self) -> dict:
        """Create initial JSON structure following dnipro.json schema"""
        return {
            "regionId": self.region_id,
            "lastUpdated": datetime.utcnow().isoformat() + "Z",
            "fact": {
                "data": {},
                "update": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "today": int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
            },
            "preset": self.get_preset_data(),
            "lastUpdateStatus": {
                "status": "parsed",
                "ok": True,
                "code": 200,
                "message": None,
                "at": datetime.utcnow().isoformat() + "Z",
                "attempt": 1
            },
            "meta": {
                "schemaVersion": "1.0.0",
                "contentHash": ""
            },
            "regionAffiliation": "Дніпро та обл."
        }

    def get_preset_data(self) -> dict:
        """Return preset data structure"""
        return {
            "days": {
                "1": "Понеділок", "2": "Вівторок", "3": "Середа", "4": "Четвер",
                "5": "П'ятниця", "6": "Субота", "7": "Неділя"
            },
            "days_mini": {
                "1": "Пн", "2": "Вт", "3": "Ср", "4": "Чт",
                "5": "Пт", "6": "Сб", "7": "Нд"
            },
            "sch_names": {
                "GPV1.1": "Черга 1.1", "GPV1.2": "Черга 1.2",
                "GPV2.1": "Черга 2.1", "GPV2.2": "Черга 2.2",
                "GPV3.1": "Черга 3.1", "GPV3.2": "Черга 3.2",
                "GPV4.1": "Черга 4.1", "GPV4.2": "Черга 4.2",
                "GPV5.1": "Черга 5.1", "GPV5.2": "Черга 5.2",
                "GPV6.1": "Черга 6.1", "GPV6.2": "Черга 6.2"
            },
            "time_zone": {str(i): [f"{i-1:02d}-{i:02d}", f"{i-1:02d}:00", f"{i:02d}:00"] 
                          for i in range(1, 25)},
            "time_type": {
                "yes": "Світло є",
                "maybe": "Можливо відключення",
                "no": "Світла немає",
                "first": "Світла не буде перші 30 хв.",
                "second": "Світла не буде другі 30 хв",
                "mfirst": "Світла можливо не буде перші 30 хв.",
                "msecond": "Світла можливо не буде другі 30 хв"
            },
            "data": {},
            "updateFact": datetime.now().strftime("%d.%m.%Y %H:%M")
        }

    def get_queue_key(self, queue_number: float) -> str:
        """Convert queue number to key format (e.g., 3.1 -> 'GPV3.1')"""
        if queue_number == int(queue_number):
            return f"GPV{int(queue_number)}.1"
        else:
            parts = str(queue_number).split('.')
            return f"GPV{parts[0]}.{parts[1]}"

    def create_default_hours(self) -> Dict[str, str]:
        """Create default hours dict with all hours set to 'yes' (power available)"""
        return {str(i): "yes" for i in range(1, 25)}

    def create_hours_from_schedules(self, time_slots: List[str]) -> Dict[str, str]:
        """Create hours dictionary from time slots
        
        Args:
            time_slots: List of time ranges like ["07:00-10:00", "14:00-18:00"]
        
        Returns:
            Dictionary with hour keys (1-24) and values ('yes' or 'no')
        """
        # Start with all hours having power
        hours = self.create_default_hours()
        
        # Mark outage hours
        for slot in time_slots:
            start_time, end_time = slot.split('-')
            start_hour = int(start_time.split(':')[0])
            start_min = int(start_time.split(':')[1])
            end_hour = int(end_time.split(':')[0])
            end_min = int(end_time.split(':')[1])
            
            # Handle outages
            for hour in range(24):
                hour_start = hour
                hour_end = hour + 1
                
                # Check if this hour overlaps with outage period
                outage_start = start_hour + (start_min / 60)
                outage_end = end_hour + (end_min / 60)
                
                # Full hour outage
                if outage_start <= hour_start and outage_end >= hour_end:
                    hours[str(hour + 1)] = "no"
                # Partial hour outage - first half
                elif outage_start <= hour_start and outage_end > hour_start and outage_end < hour_start + 0.5:
                    if hours[str(hour + 1)] == "yes":
                        hours[str(hour + 1)] = "first"
                # Partial hour outage - second half
                elif outage_start >= hour_start + 0.5 and outage_start < hour_end and outage_end >= hour_end:
                    if hours[str(hour + 1)] == "yes":
                        hours[str(hour + 1)] = "second"
                # Overlaps but not fully contained - mark as no power for safety
                elif (outage_start < hour_end and outage_end > hour_start):
                    if hours[str(hour + 1)] not in ["no", "first", "second"]:
                        hours[str(hour + 1)] = "no"
        
        return hours


def main():
    parser = argparse.ArgumentParser(description='Scrape powercut schedules from Telegram channel for ALL queues')
    parser.add_argument('--url', required=True, help='Telegram channel URL (e.g., https://t.me/s/channelname)')
    parser.add_argument('--output', default='powercuts.json', help='Output JSON file path')
    parser.add_argument('--region', default='dnipro', help='Region ID')
    
    args = parser.parse_args()
    
    try:
        # Create scraper instance
        scraper = PowercutScraper(
            channel_url=args.url,
            region_id=args.region
        )
        
        # Scrape messages for ALL queues
        schedules = scraper.scrape_messages()
        
        if not schedules:
            print("No schedules found")
            sys.exit(0)
        
        # Count total queues found
        all_queues = set()
        for date_schedules in schedules.values():
            all_queues.update(date_schedules.keys())
        
        # Load existing JSON if it exists
        existing_data = None
        if os.path.exists(args.output):
            try:
                with open(args.output, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                print(f"Loaded existing data from {args.output}")
            except Exception as e:
                print(f"Warning: Could not load existing data: {e}")
        
        # Generate JSON
        data = scraper.generate_json(schedules, existing_data)
        
        # Save JSON file
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"✓ Successfully saved schedules to {args.output}")
        print(f"  Found schedules for {len(schedules)} date(s)")
        print(f"  Total queues found: {len(all_queues)} - {sorted(all_queues)}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()