# Data Dictionary

- Based on Spark types
- Descriptions of fields are omitted because they are not provided with the original datasets

# Fact Table

## Header

**From the header dataset** \
 |-- identifier: long (nullable = true) \
 |-- carrier_code: string (nullable = true) \
 |-- vessel_country_code: string (nullable = true) \
 |-- vessel_name: string (nullable = true) \
 |-- port_of_unlading: string (nullable = true) \
 |-- estimated_arrival_date: timestamp (nullable = true) \
 |-- foreign_port_of_lading_qualifier: string (nullable = true) \
 |-- foreign_port_of_lading: string (nullable = true) \
 |-- manifest_quantity: integer (nullable = true) \
 |-- manifest_unit: string (nullable = true) \
 |-- weight: long (nullable = true) \
 |-- weight_unit: string (nullable = true) \
 |-- record_status_indicator: string (nullable = true) \
 |-- place_of_receipt: string (nullable = true) \
 |-- port_of_destination: string (nullable = true) \
 |-- foreign_port_of_destination_qualifier: string (nullable = true) \
 |-- foreign_port_of_destination: string (nullable = true) \
 |-- conveyance_id_qualifier: string (nullable = true) \
 |-- conveyance_id: string (nullable = true) \
 |-- mode_of_transportation: string (nullable = true) \
 |-- actual_arrival_date: timestamp (nullable = true) \
**From the bill dataset** \
 |-- master_bol_number: string (nullable = true) \
 |-- house_bol_number: string (nullable = true) \
 |-- sub_house_bol_number: string (nullable = true) \
 |-- voyage_number: string (nullable = true) \
 |-- bill_type_code: string (nullable = true) \
 |-- manifest_number: integer (nullable = true) \
 |-- trade_update_date: timestamp (nullable = true) \
 |-- run_date: timestamp (nullable = true)

# Dimension tables

## Cargo

**From the cargo description dataset**\
 |-- identifier: long (nullable = true) \
 |-- container_number: string (nullable = true) \
 |-- sequence_number: integer (nullable = true) \
 |-- piece_count: integer (nullable = true) \
 |-- description: string (nullable = true) \
**From the hazmat and hazmat class datasets** \
 |-- hazmat_code: string (nullable = true) \
 |-- hazmat_class: string (nullable = true) \
 |-- hazmat_code_qualifier: string (nullable = true) \
 |-- hazmat_contact: string (nullable = true) \
 |-- hazmat_page_number: string (nullable = true) \
 |-- hazmat_flash_point_temperature: string (nullable = true) \
 |-- hazmat_flash_point_temperature_negative_ind: string (nullable = true) \
 |-- hazmat_flash_point_temperature_unit: string (nullable = true) \
 |-- hazmat_description: string (nullable = true) \
 |-- harmonized_number: long (nullable = true) \
 |-- harmonized_value: double (nullable = true) \
 |-- harmonized_weight: integer (nullable = true) \
 |-- harmonized_weight_unit: string (nullable = true) \
**From the tariff harmonized number dataset** \
 |-- harmonized_tariff_schedule_desc: string (nullable = true) \
 |-- general_rate_of_duty: string (nullable = true) \
 |-- special_rate_of_duty: string (nullable = true) \
 |-- column_2_rate_of_duty: string (nullable = true) \
 |-- quota_quantity: string (nullable = true) \
 |-- additional_duties: string (nullable = true)

## Contact

**From the cosignee, notified party, and shipper datasets** \
 |-- identifier: string (nullable = true) \
 |-- name: string (nullable = true) \
 |-- address_1: string (nullable = true \
 |-- address_2: string (nullable = true) \
 |-- address_3: string (nullable = true) \
 |-- address_4: string (nullable = true) \
 |-- city: string (nullable = true) \
 |-- state_province: string (nullable = true) \
 |-- zip_code: string (nullable = true) \
 |-- country_code: string (nullable = true) \
 |-- contact_name: string (nullable = true) \
 |-- comm_number_qualifier: string (nullable = true) \
 |-- comm_number: string (nullable = true) \
 |-- contact_type: string (nullable = false)

## Container 

**From the container dataset** \
 |-- identifier: long (nullable = true) \
 |-- container_number: string (nullable = true) \
 |-- equipment_description_code: string (nullable = true) \
 |-- container_length: integer (nullable = true) \
 |-- container_height: integer (nullable = true) \
 |-- container_width: integer (nullable = true) \
 |-- container_type: string (nullable = true) \
 |-- load_status: string (nullable = true) \
 |-- type_of_service: string (nullable = true) \
**From the mark dataset** \
 |-- marks_and_numbers_1: string (nullable = true) \
 |-- marks_and_numbers_2: string (nullable = true) \
 |-- marks_and_numbers_3: string (nullable = true) \
 |-- marks_and_numbers_4: string (nullable = true) \
 |-- marks_and_numbers_5: string (nullable = true) \
 |-- marks_and_numbers_6: string (nullable = true) \
 |-- marks_and_numbers_7: string (nullable = true) \
 |-- marks_and_numbers_8: string (nullable = true)
