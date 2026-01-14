# Databricks notebook source
# At the top of each notebook
dbutils.widgets.text("catalog", "demo_nnguyen")
dbutils.widgets.text("env", "dev")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")

# COMMAND ----------

"""
Bronze Layer Data Generator - Simulates SAP ERP Raw Data
Generates realistic SAP-like raw data with quality issues, duplicates, and inconsistencies
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import hashlib
import uuid
from typing import List, Dict, Tuple
import json

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql("USE SCHEMA smartstock")

# COMMAND ----------

class BronzeDataGenerator:
    """Generates SAP-like raw data for bronze layer with realistic data quality issues."""
    
    def __init__(self):
        """Initialize the bronze data generator."""
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=3*365)
        
        # SAP-like system identifiers
        self.source_systems = {
            'PRD': 'SAP_PRODUCTION_HII',
            'QAS': 'SAP_QUALITY_ASSURANCE',
            'DEV': 'SAP_DEVELOPMENT'
        }
        
        # SAP Movement Types (BWART) - realistic SAP codes
        self.movement_types = {
            '101': {'type': 'inbound', 'desc': 'GR Goods Receipt for PO', 'frequency': 0.00},
            '102': {'type': 'inbound', 'desc': 'GR Reversal', 'frequency': 0.00},
            '122': {'type': 'inbound', 'desc': 'Return Delivery to Vendor', 'frequency': 0.00},
            '201': {'type': 'sale', 'desc': 'Goods Issue for Cost Center', 'frequency': 0.46},
            '221': {'type': 'sale', 'desc': 'Goods Issue for Project', 'frequency': 0.23},
            '261': {'type': 'sale', 'desc': 'Goods Issue for Order', 'frequency': 0.23},
            '262': {'type': 'inbound', 'desc': 'Reversal GI for Order', 'frequency': 0.00},
            '301': {'type': 'adjustment', 'desc': 'Transfer Posting Plant to Plant', 'frequency': 0.04},
            '311': {'type': 'adjustment', 'desc': 'Transfer Posting Storage Loc to Storage Loc', 'frequency': 0.03},
            '701': {'type': 'adjustment', 'desc': 'Goods Receipt from Blocked', 'frequency': 0.005},
            '702': {'type': 'adjustment', 'desc': 'Goods Issue to Blocked', 'frequency': 0.005},
            '711': {'type': 'adjustment', 'desc': 'Posting Change in Stock - Phys Inv', 'frequency': 0.00}
        }
        
        # SAP Plants (WERKS) - HII Shipbuilding Facilities
        self.plants = {
            'VA01': {'name': 'Newport News Shipyard', 'country': 'US', 'city': 'Newport News'},
            'MS01': {'name': 'Ingalls Shipbuilding', 'country': 'US', 'city': 'Pascagoula'},
            'VA02': {'name': 'Hampton Roads Supply Depot', 'country': 'US', 'city': 'Hampton'}
        }
        
        # Storage Locations (LGORT)
        self.storage_locations = ['0001'] # only one storage location for demo purposes
        
        # Product categories mapping - Frigate/Shipbuilding Materials
        self.product_categories = {
            'HULL': {'matkl': 'HUL', 'mtart': 'HAWA', 'count': 5},
            'PIPING': {'matkl': 'PIP', 'mtart': 'HAWA', 'count': 5},
            'ELECTRICAL': {'matkl': 'ELE', 'mtart': 'HAWA', 'count': 5},
            'MECHANICAL': {'matkl': 'MEC', 'mtart': 'HAWA', 'count': 5},
            'COATING': {'matkl': 'COA', 'mtart': 'HAWA', 'count': 4},
            'FASTENER': {'matkl': 'FST', 'mtart': 'NORM', 'count': 5},
            'HVAC': {'matkl': 'HVC', 'mtart': 'HAWA', 'count': 4},
            'AUXILIARY': {'matkl': 'AUX', 'mtart': 'NORM', 'count': 8}
        }
        
        self.materials = []
        self.batch_counter = 0

        self.inventory_levels = {}  # Track inventory
        self.reorder_history = {}   # Track last reorder dates

    def initialize_inventory(self):
        """Initialize inventory levels after materials are generated."""
        for material in self.materials:
            # Get reorder level from category mapping - Shipbuilding materials
            category = material['category']
            base_reorder = {
                'HULL': 15, 'PIPING': 30, 'ELECTRICAL': 35,
                'MECHANICAL': 25, 'COATING': 20, 'FASTENER': 50,
                'HVAC': 20, 'AUXILIARY': 40
            }.get(category, 20)

            # Assign each material to a health tier based on hash
            material_hash = hash(material['matnr']) % 100
            
            for plant_idx, plant in enumerate(self.plants.keys()):
                # Warehouse capacity multipliers
                if plant_idx == 0: # VA01 Newport News - high capacity (main shipyard)
                    capacity_mult = random.uniform(1.2, 1.6)
                elif plant_idx == 1: # MS01 Ingalls - medium
                    capacity_mult = random.uniform(0.9, 1.3)
                else:  # VA02 Hampton Roads Supply Depot
                    capacity_mult = random.uniform(0.7, 1.1)

                for lgort in self.storage_locations:
                    key = (material['matnr'], plant, lgort)
                    
                    # Create tiered inventory health
                    if material_hash < 10:  # 10% - CRITICAL (will stockout within 30 days)
                        base_stock = base_reorder * random.uniform(0.5, 1.5)
                    elif material_hash < 25:  # 15% - URGENT (below reorder in 30 days)
                        base_stock = base_reorder * random.uniform(1.5, 2.5)
                    elif material_hash < 45:  # 20% - ATTENTION (below reorder in 60 days)
                        base_stock = base_reorder * random.uniform(2.5, 4.0)
                    else:  # 55% - HEALTHY
                        base_stock = base_reorder * random.uniform(4.0, 8.0)

                    self.inventory_levels[key] = int(base_stock * capacity_mult)
        
    def generate_material_number(self, category: str, index: int) -> str:
        """Generate SAP-like material number (MATNR)."""
        category_prefix = self.product_categories[category]['matkl']
        # Format: MATKL + 7-digit number (e.g., MOT0000001)
        return f"{category_prefix}{str(index).zfill(7)}"
    
    def generate_batch_id(self) -> str:
        """Generate unique batch ID for data ingestion."""
        self.batch_counter += 1
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"BATCH_{timestamp}_{str(self.batch_counter).zfill(6)}"
    
    def get_ingestion_metadata(self, source_system: str = 'PRD') -> Dict:
        """Generate ingestion metadata."""
        return {
            '_source_system': self.source_systems[source_system],
            '_ingestion_time': datetime.now(),
            '_batch_id': self.generate_batch_id()
        }
    
    def generate_bronze_mara(self) -> pd.DataFrame:
        """
        Generate SAP MARA table (Material Master General Data).
        Includes realistic data quality issues: duplicates, nulls, inconsistencies.
        """
        materials = []
        material_counter = 1
        
        # Product definitions - Frigate/Shipbuilding Materials for HII
        product_definitions = [
            # Hull Materials
            {'category': 'HULL', 'name': 'Steel Plate HY-80 1in', 'desc': 'High-yield steel plate 1 inch thick for hull construction', 'weight': 245.0},
            {'category': 'HULL', 'name': 'Steel Plate HY-80 0.5in', 'desc': 'High-yield steel plate 0.5 inch thick for superstructure', 'weight': 122.5},
            {'category': 'HULL', 'name': 'Hull Stiffener T-Bar', 'desc': 'T-bar stiffener for hull longitudinal reinforcement', 'weight': 85.0},
            {'category': 'HULL', 'name': 'Bulkhead Panel Assembly', 'desc': 'Pre-fabricated watertight bulkhead panel', 'weight': 450.0},
            {'category': 'HULL', 'name': 'Keel Section Forging', 'desc': 'Forged keel section for frigate main structure', 'weight': 1200.0},
            
            # Piping & Valves
            {'category': 'PIPING', 'name': 'Seawater Pipe 6in CuNi', 'desc': 'Copper-nickel seawater piping 6 inch diameter', 'weight': 28.5},
            {'category': 'PIPING', 'name': 'Fuel Line Pipe 4in SS', 'desc': 'Stainless steel fuel line piping 4 inch diameter', 'weight': 18.2},
            {'category': 'PIPING', 'name': 'Gate Valve 6in Bronze', 'desc': 'Bronze gate valve for seawater systems', 'weight': 45.0},
            {'category': 'PIPING', 'name': 'Check Valve 4in SS', 'desc': 'Stainless steel check valve for fuel systems', 'weight': 22.5},
            {'category': 'PIPING', 'name': 'Pipe Flange Set 6in', 'desc': 'ANSI 150 flange set with gaskets and bolts', 'weight': 12.8},
            
            # Electrical Systems
            {'category': 'ELECTRICAL', 'name': 'Power Cable 4/0 AWG', 'desc': 'Marine-grade power cable 4/0 AWG per 100ft', 'weight': 85.0},
            {'category': 'ELECTRICAL', 'name': 'Switchgear Panel 450V', 'desc': 'Main distribution switchgear panel 450V', 'weight': 680.0},
            {'category': 'ELECTRICAL', 'name': 'Cable Harness Navigation', 'desc': 'Navigation systems cable harness assembly', 'weight': 12.5},
            {'category': 'ELECTRICAL', 'name': 'Junction Box Watertight', 'desc': 'Watertight junction box for deck installations', 'weight': 8.2},
            {'category': 'ELECTRICAL', 'name': 'Transformer 480V/120V', 'desc': 'Ship service transformer 50kVA', 'weight': 245.0},
            
            # Mechanical Components
            {'category': 'MECHANICAL', 'name': 'Seawater Pump 500GPM', 'desc': 'Centrifugal seawater pump 500 GPM capacity', 'weight': 185.0},
            {'category': 'MECHANICAL', 'name': 'Fuel Transfer Pump', 'desc': 'Positive displacement fuel transfer pump', 'weight': 125.0},
            {'category': 'MECHANICAL', 'name': 'Heat Exchanger Shell', 'desc': 'Shell and tube heat exchanger for cooling systems', 'weight': 320.0},
            {'category': 'MECHANICAL', 'name': 'Reduction Gearbox', 'desc': 'Main propulsion reduction gearbox assembly', 'weight': 2800.0},
            {'category': 'MECHANICAL', 'name': 'Shaft Seal Assembly', 'desc': 'Propeller shaft seal and bearing assembly', 'weight': 145.0},
            
            # Coatings & Paints
            {'category': 'COATING', 'name': 'Anti-Fouling Paint 5gal', 'desc': 'Copper-based anti-fouling hull paint', 'weight': 28.5},
            {'category': 'COATING', 'name': 'Epoxy Primer 5gal', 'desc': 'Two-part epoxy primer for steel surfaces', 'weight': 32.0},
            {'category': 'COATING', 'name': 'Deck Non-Skid Coating', 'desc': 'Non-skid deck coating system per gallon', 'weight': 12.5},
            {'category': 'COATING', 'name': 'Fire Retardant Paint', 'desc': 'Intumescent fire retardant coating 5gal', 'weight': 35.0},
            
            # Fasteners & Hardware
            {'category': 'FASTENER', 'name': 'Hex Bolt SS 1/2-13x2', 'desc': 'Stainless steel hex bolt 1/2-13 x 2in box/100', 'weight': 8.5},
            {'category': 'FASTENER', 'name': 'Hex Nut SS 1/2-13', 'desc': 'Stainless steel hex nut 1/2-13 box/100', 'weight': 3.2},
            {'category': 'FASTENER', 'name': 'Flat Washer SS 1/2', 'desc': 'Stainless steel flat washer 1/2in box/100', 'weight': 1.8},
            {'category': 'FASTENER', 'name': 'Gasket Set Flange', 'desc': 'Assorted flange gasket set EPDM/Viton', 'weight': 2.5},
            {'category': 'FASTENER', 'name': 'Rivet Set Marine', 'desc': 'Marine aluminum rivet set assorted sizes', 'weight': 4.2},
            
            # HVAC Systems
            {'category': 'HVAC', 'name': 'Air Handler Unit 10ton', 'desc': 'Ship air handling unit 10 ton cooling capacity', 'weight': 420.0},
            {'category': 'HVAC', 'name': 'Ductwork Section 24in', 'desc': 'Galvanized steel ductwork 24in diameter 10ft', 'weight': 45.0},
            {'category': 'HVAC', 'name': 'Chilled Water Coil', 'desc': 'Copper chilled water coil for AHU', 'weight': 85.0},
            {'category': 'HVAC', 'name': 'Ventilation Fan Axial', 'desc': 'Axial ventilation fan 5000 CFM capacity', 'weight': 125.0},
            
            # Auxiliary Equipment
            {'category': 'AUXILIARY', 'name': 'Anchor Chain 2.5in', 'desc': 'Studlink anchor chain 2.5 inch per shot', 'weight': 2400.0},
            {'category': 'AUXILIARY', 'name': 'Mooring Line 8in Nylon', 'desc': 'Double-braided nylon mooring line 8in x 600ft', 'weight': 185.0},
            {'category': 'AUXILIARY', 'name': 'Life Raft 25-Person', 'desc': 'SOLAS-approved 25-person life raft', 'weight': 145.0},
            {'category': 'AUXILIARY', 'name': 'Fire Hose Assembly', 'desc': 'Fire hose 2.5in x 50ft with nozzle', 'weight': 18.5},
            {'category': 'AUXILIARY', 'name': 'Davit Crane 2-Ton', 'desc': 'Boat davit crane 2-ton capacity', 'weight': 850.0},
            {'category': 'AUXILIARY', 'name': 'Navigation Light Set', 'desc': 'LED navigation light set port/starboard/stern', 'weight': 12.5},
            {'category': 'AUXILIARY', 'name': 'Watertight Door Assembly', 'desc': 'Quick-acting watertight door with frame', 'weight': 285.0},
            {'category': 'AUXILIARY', 'name': 'Insulation Blanket Marine', 'desc': 'Mineral wool insulation blanket per roll', 'weight': 22.5}
        ]
        
        base_date = datetime(2021, 12, 1, 9, 0, 0)
        
        for i, prod in enumerate(product_definitions):
            matnr = self.generate_material_number(prod['category'], material_counter)
            category = prod['category']
            
            # Introduce data quality issues (5% of records have issues) (Skipped now for the demo)
            # has_quality_issue = random.random() < 0.05
            
            material = {
                'MATNR': matnr,
                # 'MAKTX': prod['name'] if not (has_quality_issue and random.random() < 0.3) else None,  # 1.5% null names
                'MAKTX': prod['name'],
                'MEINS': 'PCE' if prod['category'] != 'ACCESSORY' else random.choice(['PCE', 'SET', 'KIT']),
                'MTART': self.product_categories[category]['mtart'],
                'MATKL': self.product_categories[category]['matkl'],
                # 'BRGEW': prod['weight'] if not (has_quality_issue and random.random() < 0.2) else None,  # 1% null weights
                'BRGEW': prod['weight'],
                'GEWEI': 'KG',
                'ERSDA': (base_date + timedelta(hours=i)).strftime('%Y%m%d'),
                'LAEDA': (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y%m%d'),
                'ERNAM': random.choice(['JSMITH', 'MJONES', 'RWILSON', 'KBROWN']),
            }
            
            # Add ingestion metadata
            material.update(self.get_ingestion_metadata())
            
            materials.append(material)
            self.materials.append({'matnr': matnr, 'category': category, 'name': prod['name']})
            material_counter += 1
            
            # Introduce duplicates (2% chance)
            if random.random() < 0.02:
                duplicate = material.copy()
                duplicate['_batch_id'] = self.generate_batch_id()
                duplicate['_ingestion_time'] = datetime.now() + timedelta(minutes=random.randint(1, 60))
                # Slight variation in duplicate to make it realistic
                if duplicate['MAKTX']:
                    duplicate['MAKTX'] = duplicate['MAKTX'] + ' '  # Trailing space
                materials.append(duplicate)
        
        return pd.DataFrame(materials)
    
    def generate_bronze_marc(self) -> pd.DataFrame:
        """Generate SAP MARC table (Material Plant Data)."""
        plant_data = []
        
        for material in self.materials:
            for plant_code in self.plants.keys():
                # Not all materials in all plants (80% coverage)
                if random.random() < 0.8:
                    
                    # Reorder levels based on category - Shipbuilding materials
                    category = material['category']
                    base_reorder = {
                        'HULL': 15, 'PIPING': 30, 'ELECTRICAL': 35,
                        'MECHANICAL': 25, 'COATING': 20, 'FASTENER': 50,
                        'HVAC': 20, 'AUXILIARY': 40
                    }.get(category, 20)
                    
                    reorder_point = base_reorder * random.uniform(0.8, 1.2)
                    
                    plant_record = {
                        'MATNR': material['matnr'],
                        'WERKS': plant_code,
                        'MINBE': round(reorder_point, 0),
                        'EISBE': round(reorder_point * 0.5, 0),  # Safety stock = 50% of reorder
                        'BSTMI': round(reorder_point * 2, 0),  # Min lot size
                        'BSTMA': round(reorder_point * 10, 0),  # Max lot size
                        'DISPO': random.choice(['001', '002', '003']),
                        'BESKZ': 'F',  # External procurement
                        'ERSDA': datetime(2022, 1, 1).strftime('%Y%m%d')
                    }
                    
                    # Add ingestion metadata
                    plant_record.update(self.get_ingestion_metadata())
                    
                    plant_data.append(plant_record)
        
        return pd.DataFrame(plant_data)
    
    def generate_bronze_mbew(self) -> pd.DataFrame:
        """Generate SAP MBEW table (Material Valuation)."""
        valuation_data = []
        
        # Price mapping - Shipbuilding materials for HII
        price_mapping = {
            # Hull Materials
            'Steel Plate HY-80 1in': 2850.00,
            'Steel Plate HY-80 0.5in': 1425.00,
            'Hull Stiffener T-Bar': 680.00,
            'Bulkhead Panel Assembly': 12500.00,
            'Keel Section Forging': 45000.00,
            # Piping & Valves
            'Seawater Pipe 6in CuNi': 485.00,
            'Fuel Line Pipe 4in SS': 320.00,
            'Gate Valve 6in Bronze': 1850.00,
            'Check Valve 4in SS': 920.00,
            'Pipe Flange Set 6in': 185.00,
            # Electrical Systems
            'Power Cable 4/0 AWG': 1250.00,
            'Switchgear Panel 450V': 28500.00,
            'Cable Harness Navigation': 3200.00,
            'Junction Box Watertight': 485.00,
            'Transformer 480V/120V': 8500.00,
            # Mechanical Components
            'Seawater Pump 500GPM': 12500.00,
            'Fuel Transfer Pump': 8200.00,
            'Heat Exchanger Shell': 18500.00,
            'Reduction Gearbox': 185000.00,
            'Shaft Seal Assembly': 6500.00,
            # Coatings & Paints
            'Anti-Fouling Paint 5gal': 285.00,
            'Epoxy Primer 5gal': 165.00,
            'Deck Non-Skid Coating': 125.00,
            'Fire Retardant Paint': 385.00,
            # Fasteners & Hardware
            'Hex Bolt SS 1/2-13x2': 85.00,
            'Hex Nut SS 1/2-13': 42.00,
            'Flat Washer SS 1/2': 28.00,
            'Gasket Set Flange': 145.00,
            'Rivet Set Marine': 68.00,
            # HVAC Systems
            'Air Handler Unit 10ton': 24500.00,
            'Ductwork Section 24in': 320.00,
            'Chilled Water Coil': 2850.00,
            'Ventilation Fan Axial': 4200.00,
            # Auxiliary Equipment
            'Anchor Chain 2.5in': 18500.00,
            'Mooring Line 8in Nylon': 2850.00,
            'Life Raft 25-Person': 8500.00,
            'Fire Hose Assembly': 485.00,
            'Davit Crane 2-Ton': 45000.00,
            'Navigation Light Set': 1850.00,
            'Watertight Door Assembly': 12500.00,
            'Insulation Blanket Marine': 185.00
        }
        
        for material in self.materials:
            for plant_code in self.plants.keys():
                # Use plant code as valuation area (BWKEY)
                bwkey = plant_code
                
                base_price = price_mapping.get(material['name'], 100.00)
                
                # Add some variance to prices across plants
                price_variance = random.uniform(0.95, 1.05)
                moving_price = round(base_price * price_variance, 2)
                
                valuation = {
                    'MATNR': material['matnr'],
                    'BWKEY': bwkey,
                    'VERPR': moving_price,
                    'STPRS': round(base_price, 2),  # Standard price (no variance)
                    'PEINH': 1,  # Price unit
                    'VPRSV': 'V',  # Moving average price control
                    'LBKUM': round(random.uniform(100, 1000), 2),  # Total valuated stock
                    'SALK3': round(moving_price * random.uniform(100, 1000), 2)  # Value of stock
                }
                
                # Add ingestion metadata
                valuation.update(self.get_ingestion_metadata())
                
                valuation_data.append(valuation)
        
        return pd.DataFrame(valuation_data)
    
    def generate_bronze_t001w(self) -> pd.DataFrame:
        """Generate SAP T001W table (Plants/Locations)."""
        plant_data = []
        
        plant_details = {
            'VA01': {
                'NAME1': 'Newport News Shipyard',
                'NAME2': 'Huntington Ingalls Industries',
                'STRAS': '4101 Washington Avenue',
                'PSTLZ': '23607',
                'ORT01': 'Newport News',
                'LAND1': 'US',
                'REGIO': 'VA'  # Virginia
            },
            'MS01': {
                'NAME1': 'Ingalls Shipbuilding',
                'NAME2': 'Huntington Ingalls Industries',
                'STRAS': '1000 Jerry St. Pe Blvd',
                'PSTLZ': '39568',
                'ORT01': 'Pascagoula',
                'LAND1': 'US',
                'REGIO': 'MS'  # Mississippi
            },
            'VA02': {
                'NAME1': 'Hampton Roads Supply Depot',
                'NAME2': 'Huntington Ingalls Industries',
                'STRAS': '2400 Industry Drive',
                'PSTLZ': '23666',
                'ORT01': 'Hampton',
                'LAND1': 'US',
                'REGIO': 'VA'  # Virginia
            }
        }
        
        for werks, details in plant_details.items():
            plant = {'WERKS': werks}
            plant.update(details)
            plant.update(self.get_ingestion_metadata())
            plant_data.append(plant)
        
        return pd.DataFrame(plant_data)
    
    def check_reorders_for_date(self, current_date, doc_counter):
        """Check inventory and generate 101 movements for items below reorder point"""
        reorder_docs = []
        
        for material in self.materials:
            category = material['category']
            reorder_level = {
                'HULL': 15, 'PIPING': 30, 'ELECTRICAL': 35,
                'MECHANICAL': 25, 'COATING': 20, 'FASTENER': 50,
                'HVAC': 20, 'AUXILIARY': 40
            }.get(category, 20)


            # Check material health tier
            material_hash = hash(material['matnr']) % 100

            for plant in self.plants.keys():
                for lgort in self.storage_locations:
                    key = (material['matnr'], plant, lgort)
                    current_inv = self.inventory_levels.get(key, 0)
                    
                    if current_inv <= reorder_level * 3:
                        # Skip reorders based on tier - let poorly managed items run out!
                        if material_hash < 10:  # CRITICAL tier (10%)
                            if random.random() < 0.95:  # Skip 95% of reorders!
                                continue
                        elif material_hash < 25:  # URGENT tier (15%)
                            if random.random() < 0.85:  # Skip 85%
                                continue
                        elif material_hash < 45:  # ATTENTION tier (20%)
                            if random.random() < 0.65:  # Skip 65%
                                continue
                        elif material_hash < 70:  # MEDIUM tier (25%)
                            if random.random() < 0.30:  # Skip 30%
                                continue
                        # HEALTHY tier (30%) - no skip, always reorder

                        last_reorder = self.reorder_history.get(key)

                        can_reorder = False
                        if last_reorder is None:
                            can_reorder = True
                        else:
                            days_since = (current_date - last_reorder).days
                            can_reorder = days_since >= 14

                        if can_reorder:
                            if current_inv == 0:
                                # CRITICAL: Complete stockout
                                # target_stock = reorder_level * random.uniform(25.0, 40.0)
                                qty = random.randint(200, 300)
                            elif current_inv < reorder_level * 2:
                                # URGENT: Very low stock
                                # target_stock = reorder_level * random.uniform(20.0, 35.0)
                                qty = random.randint(150, 250)
                            elif current_inv < reorder_level * 5:
                                # LOW: Below reorder point
                                # target_stock = reorder_level * random.uniform(15.0, 30.0)
                                qty = random.randint(100, 180)
                            else:
                                # NORMAL: Proactive reorder
                                # target_stock = reorder_level * random.uniform(10.0, 25.0)
                                qty = random.randint(80, 120)

                            # Poorly managed tiers get MUCH smaller orders
                            if material_hash < 10:  # CRITICAL
                                qty = int(qty * 0.2)  # Only 20% of normal order
                            elif material_hash < 25:  # URGENT
                                qty = int(qty * 0.35)  # Only 35%
                            elif material_hash < 45:  # ATTENTION
                                qty = int(qty * 0.55)  # Only 55%

                            # Generate reorder (movement type 101)
                            # qty = max(100, int(target_stock - current_inv))  # Minimum 50 units
                            reorder_docs.append({
                                'material': material,
                                'plant': plant,
                                'lgort': lgort,
                                'quantity': qty,
                                'date': current_date,
                                'urgency': 'CRITICAL' if current_inv == 0 else
                                          'URGENT' if current_inv < reorder_level * 0.5 else
                                          'HIGH' if current_inv < reorder_level else 'NORMAL'
                            })
                            self.inventory_levels[key] += qty
                            self.reorder_history[key] = current_date
        
        return reorder_docs

    def generate_bronze_mkpf_mseg(self, num_days: int = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generate SAP MKPF (Material Document Header) and MSEG (Material Document Items).
        This simulates the transaction data with realistic SAP characteristics.
        """
        if num_days is None:
            num_days = (self.end_date - self.start_date).days
        
        mkpf_records = []
        mseg_records = []
        
        doc_counter = 1
        current_date = self.start_date
        
        print(f"Generating {num_days} days of transaction data...")
        
        # Seasonal patterns
        seasonal_patterns = {
            1: 0.6, 2: 0.7, 3: 0.9, 4: 1.2, 5: 1.4, 6: 1.5,
            7: 1.3, 8: 1.1, 9: 1.0, 10: 0.8, 11: 0.6, 12: 0.5
        }
        
        # Growth trends (future years default to 1.2)
        growth_trends = {2022: 0.7, 2023: 0.9, 2024: 1.0, 2025: 1.1}
        
        # Day of week patterns
        dow_patterns = {0: 0.3, 1: 0.8, 2: 1.0, 3: 1.0, 4: 1.2, 5: 0.4, 6: 0.2}
        
        for day in range(num_days):
            if day % 100 == 0:
                progress = (day / num_days) * 100
                print(f"Progress: {progress:.1f}% - Processing {current_date.date()}")
            
            # Calculate daily activity level
            seasonal = seasonal_patterns[current_date.month]
            growth = growth_trends.get(current_date.year, 1.2)  # Default 1.2 for future years
            dow = dow_patterns[current_date.weekday()]
            
            daily_activity = seasonal * growth * dow
            base_transactions = int(50 * daily_activity)  # Base number of documents per day
            num_transactions = max(1, int(np.random.poisson(base_transactions)))
            
            # Check for reorders (business days only)
            if current_date.weekday() < 5:  # Monday=0, Friday=4
                reorders = self.check_reorders_for_date(current_date, doc_counter)
                
                # Generate MKPF/MSEG records for reorders
                for reorder in reorders:
                    mblnr = str(doc_counter).zfill(10)
                    mjahr = str(current_date.year)
                    
                    # Generate transaction time
                    hour = random.randint(6, 18)
                    minute = random.randint(0, 59)
                    trans_time = current_date.replace(hour=hour, minute=minute, second=0)
                    
                    # MKPF Header for reorder
                    mkpf = {
                        'MBLNR': mblnr,
                        'MJAHR': mjahr,
                        'BLDAT': current_date.strftime('%Y%m%d'),
                        'BUDAT': current_date.strftime('%Y%m%d'),
                        'USNAM': 'REORDER',
                        'TCODE': 'MIGO',
                        'BKTXT': f"Reorder - Stock below level",
                        'CPUDT': trans_time.strftime('%Y%m%d'),
                        'CPUTM': trans_time.strftime('%H%M%S'),
                        '_is_deleted': False
                    }
                    mkpf.update(self.get_ingestion_metadata())
                    mkpf_records.append(mkpf)
                    
                    # MSEG Item for reorder
                    mseg = {
                        'MBLNR': mblnr,
                        'MJAHR': mjahr,
                        'ZEILE': '0001',
                        'BWART': '101',  # GR for PO
                        'MATNR': reorder['material']['matnr'],
                        'WERKS': reorder['plant'],
                        'LGORT': reorder['lgort'],
                        'CHARG': '',
                        'MENGE': reorder['quantity'],
                        'MEINS': 'PCE',
                        'SHKZG': 'S',
                        'SOBKZ': '',
                        'GRUND': '',
                        'SGTXT': 'Reorder - Inventory replenishment',
                        'CPUDT_MKPF': trans_time.strftime('%Y%m%d'),
                        'CPUTM_MKPF': trans_time.strftime('%H%M%S')
                    }
                    mseg.update(self.get_ingestion_metadata())
                    hash_string = f"{mblnr}{mjahr}1101{reorder['material']['matnr']}{reorder['plant']}{trans_time}"
                    mseg['_record_hash'] = hashlib.md5(hash_string.encode()).hexdigest()
                    mseg_records.append(mseg)
                    
                    doc_counter += 1


            for trans in range(num_transactions):
                # Generate material document number (MBLNR) - 10 digits
                mblnr = str(doc_counter).zfill(10)
                mjahr = str(current_date.year)
                
                # Select movement type
                movement_types = list(self.movement_types.keys())
                movement_weights = [self.movement_types[mt]['frequency'] for mt in movement_types]
                bwart = random.choices(movement_types, weights=movement_weights)[0]
                
                # Generate transaction time
                hour = random.choices(range(6, 19), weights=[0.5, 1.0, 1.0, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.5])[0]
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                trans_time = current_date.replace(hour=hour, minute=minute, second=second)
                
                # MKPF Header
                mkpf = {
                    'MBLNR': mblnr,
                    'MJAHR': mjahr,
                    'BLDAT': current_date.strftime('%Y%m%d'),  # Document date
                    'BUDAT': current_date.strftime('%Y%m%d'),  # Posting date
                    'USNAM': random.choice(['JSMITH', 'MJONES', 'RWILSON', 'KBROWN', 'LDAVIS']),
                    'TCODE': random.choice(['MIGO', 'MB1A', 'MB1B', 'MB1C']),
                    'BKTXT': f"Mat Doc {mblnr}",
                    'CPUDT': trans_time.strftime('%Y%m%d'),
                    'CPUTM': trans_time.strftime('%H%M%S'),
                    '_is_deleted': False  # Soft delete flag
                }
                
                # Add ingestion metadata
                mkpf.update(self.get_ingestion_metadata())
                
                # Introduce soft deletes (1% chance)
                if random.random() < 0.01:
                    mkpf['_is_deleted'] = True
                
                mkpf_records.append(mkpf)
                
                # Generate MSEG items (1-3 items per document)
                num_items = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
                
                for item_num in range(1, num_items + 1):
                    # Select random material and plant
                    material = random.choice(self.materials)
                    plant = random.choice(list(self.plants.keys()))
                    storage_loc = random.choice(self.storage_locations)


                    # Generate inventory-aware quantity
                    current_inv = self.inventory_levels.get((material['matnr'], plant, storage_loc), 0)
                    reorder_level = material.get('reorder_level', 20)

                    if self.movement_types[bwart]['type'] == 'inbound':
                        quantity = random.randint(50, 400)
                        shkzg = 'S'
                    elif self.movement_types[bwart]['type'] == 'sale':
                        max_qty = min(current_inv, 10)
                        quantity = random.randint(1, max(1, max_qty)) if max_qty > 0 else 0
                        shkzg = 'H'
                    else:  # adjustment
                        quantity = random.randint(1, 10)
                        shkzg = random.choice(['S', 'H'])

                    # Update inventory tracking
                    if quantity > 0:
                        change = quantity if shkzg == 'S' else -quantity
                        key = (material['matnr'], plant, storage_loc)
                        self.inventory_levels[key] = max(0, current_inv + change)

                    # Generate quantity based on movement type
                    '''if self.movement_types[bwart]['type'] == 'inbound':
                        quantity = random.randint(10, 50)
                        shkzg = 'S'  # Credit
                    elif self.movement_types[bwart]['type'] == 'sale':
                        quantity = random.randint(5, 25)
                        shkzg = 'H'  # Debit
                    else:  # adjustment
                        quantity = random.randint(1, 10)
                        shkzg = random.choice(['S', 'H'])
                    '''
                    
                    
                    # Generate batch number (CHARG) for some items
                    charg = f"B{current_date.strftime('%Y%m')}{str(random.randint(1, 999)).zfill(3)}" if random.random() < 0.3 else ''
                    
                    mseg = {
                        'MBLNR': mblnr,
                        'MJAHR': mjahr,
                        'ZEILE': str(item_num).zfill(4),  # Line item
                        'BWART': bwart,
                        'MATNR': material['matnr'],
                        'WERKS': plant,
                        'LGORT': storage_loc,
                        'CHARG': charg,
                        'MENGE': quantity,
                        'MEINS': 'PCE',
                        'SHKZG': shkzg,
                        'SOBKZ': '',  # Special stock indicator (usually empty)
                        'GRUND': random.choice(['', '0001', '0002', 'QC', 'DMG']) if random.random() < 0.1 else '',
                        'SGTXT': self.movement_types[bwart]['desc'],
                        'CPUDT_MKPF': trans_time.strftime('%Y%m%d'),
                        'CPUTM_MKPF': trans_time.strftime('%H%M%S')
                    }
                    
                    # Add ingestion metadata
                    mseg.update(self.get_ingestion_metadata())
                    
                    # Generate record hash for deduplication
                    hash_string = f"{mblnr}{mjahr}{item_num}{bwart}{material['matnr']}{plant}{trans_time}"
                    mseg['_record_hash'] = hashlib.md5(hash_string.encode()).hexdigest()
                    
                    # Introduce duplicates (3% chance)
                    mseg_records.append(mseg)
                    if random.random() < 0.03:
                        duplicate = mseg.copy()
                        duplicate['_batch_id'] = self.generate_batch_id()
                        duplicate['_ingestion_time'] = datetime.now() + timedelta(minutes=random.randint(1, 30))
                        mseg_records.append(duplicate)
                
                doc_counter += 1
            
            current_date += timedelta(days=1)
        
        print(f"Generated {len(mkpf_records)} material documents with {len(mseg_records)} line items")
        
        return pd.DataFrame(mkpf_records), pd.DataFrame(mseg_records)

# COMMAND ----------

generator = BronzeDataGenerator()

# COMMAND ----------

df_mara = generator.generate_bronze_mara()
display(df_mara)

# COMMAND ----------

spark_df_mara = spark.createDataFrame(df_mara)
spark_df_mara.write.mode('overwrite').saveAsTable('bronze_mara')


# COMMAND ----------

# Initialize inventory after materials are generated
generator.initialize_inventory()
print("✅ Inventory initialized")

# COMMAND ----------

df_marc = generator.generate_bronze_marc()
display(df_marc)

# COMMAND ----------

spark_df_marc = spark.createDataFrame(df_marc)
spark_df_marc.write.mode('overwrite').saveAsTable('bronze_marc')

# COMMAND ----------

df_mbew = generator.generate_bronze_mbew()
display(df_mbew)

# COMMAND ----------

spark_df_mbew = spark.createDataFrame(df_mbew)
spark_df_mbew.write.mode("overwrite").saveAsTable("bronze_mbew")

# COMMAND ----------

df_t001w = generator.generate_bronze_t001w()
display(df_t001w)

# COMMAND ----------

spark_df_t001w = spark.createDataFrame(df_t001w)
spark_df_t001w.write.mode("overwrite").saveAsTable("bronze_t001w")

# COMMAND ----------

df_mkpf, df_mseg = generator.generate_bronze_mkpf_mseg()
display(df_mkpf)
display(df_mseg)

# COMMAND ----------

spark_df_mkpf = spark.createDataFrame(df_mkpf)
spark_df_mkpf.write.mode("overwrite").saveAsTable("bronze_mkpf")


spark_df_mseg = spark.createDataFrame(df_mseg)
spark_df_mseg.write.mode("overwrite").saveAsTable("bronze_mseg") 

# COMMAND ----------

print("\n=== REORDER TRANSACTION CHECK ===")
reorder_count = len(df_mseg[df_mseg['BWART'] == '101'])
reorder_qty = df_mseg[df_mseg['BWART'] == '101']['MENGE'].sum() if reorder_count > 0 else 0

print(f"Total 101 reorder transactions: {reorder_count}")
print(f"Total reorder quantity: {reorder_qty:,}")

sale_count = len(df_mseg[df_mseg['BWART'].isin(['201', '221', '261'])])
sale_qty = df_mseg[df_mseg['BWART'].isin(['201', '221', '261'])]['MENGE'].sum()

print(f"Total sale transactions: {sale_count}")
print(f"Total sale quantity: {sale_qty:,}")
print(f"Net flow: {reorder_qty - sale_qty:,}")