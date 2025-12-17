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
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
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

    def cleanup_old_data(self, data: dict) -> dict:
        """Remove data for dates before today (Kyiv timezone)
        
        Args:
            data: The JSON data structure to clean up
            
        Returns:
            Cleaned data with only today and future dates
        """
        try:
            # Get current time in Kyiv timezone
            kyiv_tz = ZoneInfo('Europe/Kyiv')
            now_kyiv = datetime.now(kyiv_tz)
            today_midnight_kyiv = now_kyiv.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Convert to Unix timestamp for comparison
            today_timestamp = int(today_midnight_kyiv.timestamp())
            
            print(f"Cleanup: Current Kyiv time: {now_kyiv.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"Cleanup: Today midnight timestamp: {today_timestamp}")
            
            # Get all timestamps from the data
            if "fact" not in data or "data" not in data["fact"]:
                print("Cleanup: No fact data found in JSON")
                return data
            
            old_timestamps = list(data["fact"]["data"].keys())
            
            # Remove timestamps older than today
            removed_count = 0
            for timestamp_str in old_timestamps:
                timestamp = int(timestamp_str)
                if timestamp < today_timestamp:
                    del data["fact"]["data"][timestamp_str]
                    removed_count += 1
                    # Convert timestamp to readable date
                    date_obj = datetime.fromtimestamp(timestamp, tz=kyiv_tz)
                    print(f"Cleanup: Removed old data for {date_obj.strftime('%Y-%m-%d')}")
            
            if removed_count == 0:
                print("Cleanup: No old data to remove")
            else:
                print(f"Cleanup: Removed {removed_count} old date(s)")
            
            # Update the today field
            data["fact"]["today"] = today_timestamp
            
            return data
            
        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")
            return data

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
        
        # Find all message widgets (full message containers, not just text)
        message_widgets = soup.find_all('div', class_='tgme_widget_message')

        # Get today's date in Kyiv timezone
        kyiv_tz = ZoneInfo('Europe/Kyiv')
        today = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        today_str = today.strftime("%d.%m.%Y")

        # Collect all schedules: date -> queue_number -> time_slots
        all_schedules = {}
        modifications_by_date = {}
        
        for message_widget in message_widgets:
            # Extract message timestamp from datetime attribute
            message_timestamp = self.extract_message_timestamp(message_widget)
            
            # Get message text
            message_text_div = message_widget.find('div', class_='tgme_widget_message_text')
            if not message_text_div:
                continue
                
            message_text = message_text_div.get_text()
            
            # Extract schedules for ALL queues
            schedules_by_queue = self.extract_all_schedules(message_text)
            
            # Extract date from message - if not found, assume today for modifications
            date = self.extract_date(message_text)
            
            # If no date found but message has modifications, use today's date
            if not date and schedules_by_queue:
                # Check if this message contains only modifications
                has_modifications = any(
                    any(slot.startswith("MOD:") for slot in slots)
                    for slots in schedules_by_queue.values()
                )
                if has_modifications:
                    date = today_str
                    print(f"No date in modification message, using today: {date}")
            
            if date and schedules_by_queue:
                date_obj = datetime.strptime(date, "%d.%m.%Y")
                # Make date_obj timezone-aware
                date_obj = date_obj.replace(tzinfo=kyiv_tz)
                
                # Only process today or future dates
                if date_obj >= today:
                    if date not in all_schedules:
                        all_schedules[date] = {}
                    if date not in modifications_by_date:
                        modifications_by_date[date] = {}
                    
                    # Separate regular schedules from modifications
                    for queue_num, time_slots in schedules_by_queue.items():
                        regular_slots = []
                        mods = []
                        
                        for slot in time_slots:
                            if slot.startswith("MOD:"):
                                # Only accept modifications if message was posted today
                                if message_timestamp and self.is_message_from_today(message_timestamp, kyiv_tz):
                                    mods.append(slot)
                                    msg_time = message_timestamp.strftime('%H:%M')
                                    print(f"Accepting modification for queue {queue_num} from today's message ({msg_time}): {slot}")
                                else:
                                    if message_timestamp:
                                        msg_date = message_timestamp.strftime('%Y-%m-%d %H:%M')
                                        print(f"Skipping modification for queue {queue_num} from {msg_date} (not today): {slot}")
                                    else:
                                        print(f"Skipping modification for queue {queue_num} (no timestamp available): {slot}")
                            else:
                                regular_slots.append(slot)
                        
                        if regular_slots:
                            combined_slots = self.combine_time_slots(date, regular_slots)
                            if combined_slots:
                                all_schedules[date][queue_num] = combined_slots
                                print(f'Found schedules for {date}, Queue {queue_num}: {combined_slots}')
                        
                        if mods:
                            if queue_num not in modifications_by_date[date]:
                                modifications_by_date[date][queue_num] = []
                            modifications_by_date[date][queue_num].extend(mods)
        
        # Apply modifications to schedules
        self.apply_modifications(all_schedules, modifications_by_date)
        
        return all_schedules
    
    def extract_message_timestamp(self, message_widget) -> Optional[datetime]:
        """Extract the timestamp when a message was posted
        
        Args:
            message_widget: BeautifulSoup element for the message widget
            
        Returns:
            datetime object in Kyiv timezone, or None if not found
        """
        try:
            # Look for the time element using the same selector as Home Assistant
            # CSS selector: .tgme_widget_message_meta a.tgme_widget_message_date time.time
            meta_div = message_widget.find('div', class_='tgme_widget_message_meta')
            if meta_div:
                date_link = meta_div.find('a', class_='tgme_widget_message_date')
                if date_link:
                    time_tag = date_link.find('time', class_='time', attrs={'datetime': True})
                    if time_tag:
                        datetime_str = time_tag['datetime']
                        # Parse ISO format datetime (e.g., "2025-12-08T10:30:00+02:00")
                        msg_datetime = datetime.fromisoformat(datetime_str)
                        
                        # Convert to Kyiv timezone
                        kyiv_tz = ZoneInfo('Europe/Kyiv')
                        msg_datetime_kyiv = msg_datetime.astimezone(kyiv_tz)
                        
                        return msg_datetime_kyiv
        except Exception as e:
            print(f"Warning: Could not extract message timestamp: {e}")
        
        return None
    
    def is_message_from_today(self, message_timestamp: datetime, kyiv_tz) -> bool:
        """Check if a message timestamp is from today (Kyiv timezone)
        
        Args:
            message_timestamp: datetime object of the message
            kyiv_tz: ZoneInfo for Kyiv timezone
            
        Returns:
            True if message is from today, False otherwise
        """
        today = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        
        return today <= message_timestamp < tomorrow
    
    def apply_modifications(self, schedules: Dict[str, Dict[float, List[str]]], 
                           modifications: Dict[str, Dict[float, List[str]]]):
        """Apply schedule modifications (prolongations, early starts, additional outages) to existing schedules
        
        Args:
            schedules: The main schedules dict to modify
            modifications: Dict of modifications to apply
        """
        for date, queue_mods in modifications.items():
            if date not in schedules:
                schedules[date] = {}
            
            for queue_num, mod_list in queue_mods.items():
                for mod in mod_list:
                    # Parse modification: "MOD:prolong:13:00" or "MOD:early_start:11:00" or "MOD:additional:06:00-09:00"
                    parts = mod.split(':', 2)  # Split into max 3 parts
                    if len(parts) >= 3:
                        mod_type = parts[1]
                        mod_data = parts[2]
                        
                        if mod_type == 'additional':
                            # Additional outage is a new time slot to add
                            if queue_num not in schedules[date]:
                                schedules[date][queue_num] = []
                            schedules[date][queue_num].append(mod_data)
                            # Recombine after adding
                            schedules[date][queue_num] = self.combine_time_slots(date, schedules[date][queue_num])
                            print(f"Added additional outage {mod_data} to queue {queue_num} on {date}")
                        
                        elif queue_num in schedules[date]:
                            existing_slots = schedules[date][queue_num]
                            modified_slots = self.modify_time_slots(
                                existing_slots, mod_type, mod_data, date
                            )
                            schedules[date][queue_num] = modified_slots
                            print(f"Applied {mod_type} modification to queue {queue_num} on {date}: {modified_slots}")
                        
                        elif mod_type == 'cancel':
                            # Remove the queue if cancelled
                            if queue_num in schedules[date]:
                                del schedules[date][queue_num]
                                print(f"Cancelled schedule for queue {queue_num} on {date}")
    
    def modify_time_slots(self, time_slots: List[str], mod_type: str, 
                         mod_time: str, date: str) -> List[str]:
        """Modify time slots based on modification type
        
        Args:
            time_slots: Existing time slots like ["07:00-10:00", "14:00-18:00"]
            mod_type: 'prolong' or 'early_start'
            mod_time: Time for modification like "13:00"
            date: Date string for parsing
        
        Returns:
            Modified list of time slots
        """
        if not time_slots:
            return time_slots
        
        current_time = datetime.now()
        
        if mod_type == 'prolong':
            # Find which slot to prolong
            # Strategy: Find the slot that is currently ongoing or the last slot that ended most recently
            slot_to_modify = None
            slot_index = None
            
            for idx, slot in enumerate(time_slots):
                start_time_str, end_time_str = slot.split('-')
                try:
                    slot_start = datetime.strptime(f"{date} {start_time_str}", "%d.%m.%Y %H:%M")
                    slot_end = datetime.strptime(f"{date} {end_time_str}", "%d.%m.%Y %H:%M")
                    
                    # Check if this slot is currently ongoing
                    if slot_start <= current_time <= slot_end:
                        slot_to_modify = idx
                        break
                    
                    # Check if this slot just ended (within last 2 hours) - might be getting extended
                    elif current_time > slot_end and (current_time - slot_end).total_seconds() < 7200:
                        slot_to_modify = idx
                except:
                    continue
            
            # If no specific slot found, extend the last one (default behavior)
            if slot_to_modify is None:
                slot_to_modify = len(time_slots) - 1
            
            # Modify the identified slot
            modified = time_slots.copy()
            start_time = modified[slot_to_modify].split('-')[0]
            modified[slot_to_modify] = f"{start_time}-{mod_time}"
            
            print(f"  Prolonging slot {slot_to_modify + 1} of {len(time_slots)}: {time_slots[slot_to_modify]} → {modified[slot_to_modify]}")
            
        elif mod_type == 'early_start':
            # Find which slot to start earlier
            # Strategy: Find the first slot that hasn't started yet or is currently ongoing
            slot_to_modify = None
            
            for idx, slot in enumerate(time_slots):
                start_time_str, end_time_str = slot.split('-')
                try:
                    slot_start = datetime.strptime(f"{date} {start_time_str}", "%d.%m.%Y %H:%M")
                    slot_end = datetime.strptime(f"{date} {end_time_str}", "%d.%m.%Y %H:%M")
                    
                    # Check if this slot hasn't started yet or is ongoing
                    if current_time <= slot_end:
                        slot_to_modify = idx
                        break
                except:
                    continue
            
            # If no specific slot found, modify the first one (default behavior)
            if slot_to_modify is None:
                slot_to_modify = 0
            
            # Modify the identified slot
            modified = time_slots.copy()
            end_time = modified[slot_to_modify].split('-')[1]
            modified[slot_to_modify] = f"{mod_time}-{end_time}"
            
            print(f"  Early start for slot {slot_to_modify + 1} of {len(time_slots)}: {time_slots[slot_to_modify]} → {modified[slot_to_modify]}")
            
        else:
            modified = time_slots
        
        # Recombine slots in case modification created overlap
        return self.combine_time_slots(date, modified)
    

    def extract_all_schedules(self, message: str) -> Dict[float, List[str]]:
        """Extract schedule time slots for ALL queue numbers found in message
        
        Returns:
            Dict mapping queue_number to list of time slots
            Example: {1.1: ["07:00-10:00"], 3.1: ["14:00-18:00"]}
        """
        schedules_by_queue = {}
        
        # Pattern: "3.1 черга: з 07:00 до 10:00; з 14:00 до 18:00"
        pattern = re.compile(r'([\d.]+)\W*черг[аи]:\s*((?:\W+з\s\d{2}:\d{2}\sдо\s\d{2}:\d{2};?)+)', re.IGNORECASE)
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
            r'(?:з\s(\d{2}:\d{2})\sдо\s(\d{2}:\d{2});?)\sвідключа[ює]ться*([0-9\sта,.;:!?]*черг[аи])+', re.IGNORECASE)
        matches = old_pattern.findall(message)

        if matches:
            for match in matches:
                start_time = match[0]
                end_time = match[1]
                time_range = f"{start_time}-{end_time}"
                queue_info = match[2]
                
                # Extract all numbers (including decimals) from queue_info
                queue_numbers = re.findall(r'[\d.]+', queue_info)
                
                for queue_str in queue_numbers:
                    queue_number = float(queue_str)
                    if queue_number not in schedules_by_queue:
                        schedules_by_queue[queue_number] = []
                    schedules_by_queue[queue_number].append(time_range)
        
        # Check for schedule modifications
        modifications = self.extract_modifications(message)
        if modifications:
            for queue_number, mod_type, mod_time in modifications:
                if queue_number not in schedules_by_queue:
                    schedules_by_queue[queue_number] = []
                # Store modification info as special time slot that will be processed later
                schedules_by_queue[queue_number].append(f"MOD:{mod_type}:{mod_time}")

        return schedules_by_queue

    def extract_modifications(self, message: str) -> List[tuple]:
        """Extract schedule modifications (prolongations, early starts, etc.)
        
        Returns:
            List of tuples: (queue_number, modification_type, time)
            modification_type: 'prolong', 'early_start', 'cancel'
        """
        modifications = []
        message_lower = message.lower()
        
        # Pattern for prolongation: "до 13:00 подовжено відключення підчерги 2.1"
        prolong_pattern = re.compile(
            r'до\s(\d{2}:\d{2})\sподовжен[оа]\s+(?:відключення\s+)?(?:під)?черг[аиу]?\s+([\d.,\s]+)', 
            re.IGNORECASE
        )
        for match in prolong_pattern.finditer(message):
            time = match.group(1)
            queues_str = match.group(2)
            # Extract all queue numbers from the string
            queue_numbers = re.findall(r'[\d.]+', queues_str)
            for queue_str in queue_numbers:
                try:
                    queue = float(queue_str)
                    modifications.append((queue, 'prolong', time))
                    print(f"Found prolongation for queue {queue} until {time}")
                except ValueError:
                    continue
        
        # Pattern for additional outage: "з 06:00 до 09:00 додатково застосовуватиметься відключення підчерг 1.1, 1.2 та 3.1"
        # This pattern handles time range (from-to) with multiple queues
        additional_pattern = re.compile(
            r'з\s(\d{2}:\d{2})(?:\sдо\s(\d{2}:\d{2}))?\s+додатково\s+(?:застосовуватиметься|застосовується)\s+(?:відключення\s+)?(?:під)?черг[аиу]?\s+([\d.,\sіта]+)',
            re.IGNORECASE
        )
        for match in additional_pattern.finditer(message):
            start_time = match.group(1)
            end_time = match.group(2) if match.group(2) else None
            queues_str = match.group(3)
            
            # Extract all queue numbers from the string (handles "1.1, 1.2 та 3.1")
            queue_numbers = re.findall(r'[\d.]+', queues_str)
            
            for queue_str in queue_numbers:
                try:
                    queue = float(queue_str)
                    # If there's an end time, this is a new outage slot, not just early start
                    if end_time:
                        modifications.append((queue, 'additional', f"{start_time}-{end_time}"))
                        print(f"Found additional outage for queue {queue} from {start_time} to {end_time}")
                    else:
                        modifications.append((queue, 'early_start', start_time))
                        print(f"Found early start for queue {queue} at {start_time}")
                except ValueError:
                    continue
        
        # Pattern for cancellation: "скасовано відключення черги 3.1"
        cancel_pattern = re.compile(
            r'скасован[оа]\s+(?:відключення\s+)?(?:під)?черг[аиу]?\s+([\d.,\s]+)',
            re.IGNORECASE
        )
        for match in cancel_pattern.finditer(message):
            queues_str = match.group(1)
            queue_numbers = re.findall(r'[\d.]+', queues_str)
            for queue_str in queue_numbers:
                try:
                    queue = float(queue_str)
                    modifications.append((queue, 'cancel', '00:00'))
                    print(f"Found cancellation for queue {queue}")
                except ValueError:
                    continue
        
        return modifications

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
        
        # Clean up old data (dates before today in Kyiv timezone)
        data = self.cleanup_old_data(data)
        
        # Update lastUpdated timestamp
        data["lastUpdated"] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
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
        
        # Set today's timestamp (using Kyiv timezone)
        kyiv_tz = ZoneInfo('Europe/Kyiv')
        today_midnight = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        data["fact"]["today"] = int(today_midnight.timestamp())
        
        # Update lastUpdateStatus
        data["lastUpdateStatus"]["at"] = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        
        return data

    def create_json_structure(self) -> dict:
        """Create initial JSON structure following dnipro.json schema"""
        kyiv_tz = ZoneInfo('Europe/Kyiv')
        today_midnight = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        
        return {
            "regionId": self.region_id,
            "lastUpdated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "fact": {
                "data": {},
                "update": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "today": int(today_midnight.timestamp())
            },
            "preset": self.get_preset_data(),
            "lastUpdateStatus": {
                "status": "parsed",
                "ok": True,
                "code": 200,
                "message": None,
                "at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
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
            Dictionary with hour keys (1-24) and values ('yes', 'no', 'first', or 'second')
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
            
            # Convert to decimal hours for easier calculation
            outage_start = start_hour + (start_min / 60.0)
            outage_end = end_hour + (end_min / 60.0)
            
            # Process each hour
            for hour in range(24):
                # Hour N in the JSON represents the time from N:00 to (N+1):00
                # But hour index in loop is 0-23, so hour_start = hour, hour_end = hour + 1
                hour_start = float(hour)
                hour_end = float(hour + 1)
                hour_mid = hour + 0.5
                
                # Skip if no overlap
                if outage_end <= hour_start or outage_start >= hour_end:
                    continue
                
                # Calculate overlap
                overlap_start = max(outage_start, hour_start)
                overlap_end = min(outage_end, hour_end)
                overlap_duration = overlap_end - overlap_start
                
                # Determine the status based on overlap
                if overlap_duration >= 1.0:
                    # Full hour outage
                    hours[str(hour + 1)] = "no"
                elif overlap_duration > 0:
                    # Partial hour outage
                    # Check if outage is in first half or second half
                    if overlap_start < hour_mid and overlap_end <= hour_mid:
                        # Outage only in first half (00-30 minutes)
                        if hours[str(hour + 1)] == "yes":
                            hours[str(hour + 1)] = "first"
                        elif hours[str(hour + 1)] == "second":
                            hours[str(hour + 1)] = "no"  # Both halves affected
                    elif overlap_start >= hour_mid and overlap_end > hour_mid:
                        # Outage only in second half (30-60 minutes)
                        if hours[str(hour + 1)] == "yes":
                            hours[str(hour + 1)] = "second"
                        elif hours[str(hour + 1)] == "first":
                            hours[str(hour + 1)] = "no"  # Both halves affected
                    else:
                        # Outage spans both halves or is ambiguous - mark as full outage
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
