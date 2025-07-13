%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 5

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from awsglue.dynamicframe import DynamicFrame
import pandas as pd
import boto3
from pyspark.sql.types import StringType, DoubleType, IntegerType, TimestampType
from awsglue.dynamicframe import DynamicFrame
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyspark.sql import functions as F
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
spark = glueContext.spark_session
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
dyf = glueContext.create_dynamic_frame.from_catalog(database='ahp_production_load_updated', table_name='final_poejulybatchall_parquet000_parquet',additional_options ={"useCatalogSchema":True})
dyf.printSchema()
df = dyf.toDF()

# Total number of rows
total_rows = df.count()

# Efficiently compute non-null and null counts for all columns in one pass
agg_exprs = []
for c in df.columns:
    agg_exprs.append(F.count(F.col(c)).alias(f"{c}_non_null"))
    agg_exprs.append(F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}_null"))

agg_df = df.agg(*agg_exprs).toPandas()

# Reshape the result for readability
col_counts = []
for c in df.columns:
    non_null = int(agg_df[f"{c}_non_null"][0])
    nulls = int(agg_df[f"{c}_null"][0])
    col_counts.append({'column': c, 'non_null_count': non_null, 'null_count': nulls})

col_counts_df = pd.DataFrame(col_counts)
s3_counts_path = "s3://dbt-us-east-2-ohio-v2/POE/output_file/column_counts/"
col_counts_df.to_csv(f"{s3_counts_path}column_counts_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
df = dyf.toDF()

# Total number of rows
total_rows = df.count()

# Efficiently compute non-null and null counts for all columns in one pass
agg_exprs = []
for c in df.columns:
    agg_exprs.append(F.count(F.col(c)).alias(f"{c}_non_null"))
    agg_exprs.append(F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}_null"))

agg_df = df.agg(*agg_exprs).toPandas()

# Reshape the result for readability
col_counts = []
for c in df.columns:
    non_null = int(agg_df[f"{c}_non_null"][0])
    nulls = int(agg_df[f"{c}_null"][0])
    col_counts.append({'column': c, 'non_null_count': non_null, 'null_count': nulls})

col_counts_df = pd.DataFrame(col_counts)
s3_counts_path = "s3://dbt-us-east-2-ohio-v2/POE/output_file/column_counts/"
col_counts_df.to_csv(f"{s3_counts_path}column_counts_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
export_metadata = {
    "production_load.l_mt_poe_data": {
        "order": [
            "poe_po_unique_id",
            "pmr_po_base_each_price",
            "pmr_po_benchmark_10th_percentile",
            "pmr_po_benchmark_25th_percentile",
            "pmr_po_benchmark_50th_percentile",
            "pmr_po_benchmark_75th_percentile",
            "pmr_po_benchmark_90th_percentile",
            "pmr_po_benchmark_high_price",
            "pmr_po_benchmark_low_price",
            "pmr_po_benchmark_median_price",
            "pmr_po_calculated_facility_base_each_price",
            "pmr_po_calculated_facility_landed_each_price",
            "pmr_po_contract_access_price",
            "pmr_po_contract_access_tier_description",
            "pmr_po_contract_best_price",
            "pmr_po_contract_best_tier_description",
            "pmr_po_contract_category",
            "pmr_po_contract_effective_date",
            "pmr_po_contract_expiration_date",
            "pmr_po_contract_group",
            "pmr_po_contract_name",
            "pmr_po_contract_number",
            "pmr_po_contract_pkg_access_price",
            "pmr_po_contract_pkg_best_price",
            "pmr_po_contract_pkg_uom",
            "pmr_po_contract_price_found",
            "pmr_po_contract_type",
            "pmr_po_contract_tagging",
            "pmr_po_contract_uom_conv",
            "pmr_po_contracted_catalog_number",
            "pmr_po_contracted_supplier",
            "pmr_po_contracted_supplier_parent",
            "pmr_po_ctr_flag",
            "pmr_info",
            "pmr_po_data_cleansing_code",
            "pmr_categorized_spend",
            "pmr_po_direct_parent_entity_code",
            "pmr_po_direct_parent_name",
            "pmr_po_distributor_markup_percent",
            "pmr_po_facility_base_price",
            "pmr_po_facility_contract_name",
            "pmr_po_facility_contract_number",
            "pmr_po_facility_contract_price",
            "pmr_po_facility_contract_type",
            "pmr_po_facility_custom_field_1",
            "pmr_po_facility_custom_field_2",
            "pmr_po_facility_custom_field_3",
            "pmr_po_facility_custom_field_4",
            "pmr_po_facility_custom_field_5",
            "pmr_po_facility_custom_field_6",
            "pmr_po_facility_custom_field_7",
            "pmr_po_facility_custom_field_8",
            "pmr_po_facility_custom_field_9",
            "pmr_po_facility_custom_field_10",
            "pmr_po_facility_dea",
            "pmr_po_facility_department_name",
            "pmr_po_facility_department_number",
            "pmr_po_facility_hin",
            "pmr_po_facility_landed_price",
            "pmr_po_facility_manufacturer_catalog_num",
            "pmr_po_facility_manufacturer_entity_code",
            "pmr_po_facility_manufacturer_name",
            "pmr_po_facility_mmis",
            "pmr_po_facility_name",
            "pmr_po_facility_pkg_uom",
            "pmr_po_facility_product_description",
            "pmr_po_facility_submitted_name",
            "pmr_po_facility_uom_conv",
            "pmr_po_facility_vendor_catalog_num",
            "pmr_po_facility_vendor_entity_code",
            "pmr_po_facility_vendor_name",
            "pmr_po_health_system_entity_code",
            "pmr_po_health_system_name",
            "pmr_po_inner_pack_quantity",
            "pmr_po_inner_pack_uom",
            "pmr_po_landed_each_price",
            "pmr_po_manufacturer_catalog_number",
            "pmr_po_reference_number_surrogate",
            "pmr_po_manufacturer_diversity_type",
            "pmr_po_manufacturer_entity_code",
            "pmr_po_manufacturer_name",
            "pmr_po_manufacturer_top_parent_entity_code",
            "pmr_po_manufacturer_top_parent_name",
            "pmr_po_mdf_date",
            "pmr_po_mdf_each_price",
            "pmr_po_mdf_found",
            "pmr_po_mdf_price",
            "pmr_po_mdf_required",
            "pmr_po_month",
            "pmr_po_packaging_string",
            "pmr_po_pin",
            "pmr_po_pkg_uom",
            "pmr_po_premier_entity_code",
            "pmr_po_product_description",
            "pmr_po_program_line",
            "pmr_po_signed_custom_tier_description",
            "pmr_po_signed_preferred_tier_indicator",
            "pmr_po_signed_tier_description",
            "pmr_po_signed_tier_status",
            "pmr_po_signed_tier_status_date",
            "pmr_po_spend_type",
            "pmr_po_transaction_date",
            "pmr_po_transaction_line_number",
            "pmr_po_transaction_number",
            "pmr_po_unspsc_class_code",
            "pmr_po_unspsc_class_description",
            "pmr_po_unspsc_commodity_code",
            "pmr_po_unspsc_commodity_description",
            "pmr_po_unspsc_family_code",
            "pmr_po_unspsc_family_description",
            "pmr_po_unspsc_segment_code",
            "pmr_po_unspsc_segment_description",
            "pmr_po_uom_conv",
            "pmr_po_vendor_entity_code",
            "pmr_po_vendor_name",
            "pmr_po_vendor_top_parent_entity_code",
            "pmr_po_vendor_top_parent_name",
            "pmr_po_base_spend_1",
            "pmr_po_landed_spend",
            "pmr_po_last_base_each_price",
            "pmr_po_last_base_price",
            "pmr_po_last_landed_each_price",
            "pmr_po_last_landed_price",
            "pmr_po_last_transaction_date",
            "pmr_po_opportunity_to_benchmark_10th_percentile",
            "pmr_po_opportunity_to_benchmark_25th_percentile",
            "pmr_po_opportunity_to_benchmark_50th_percentile",
            "pmr_po_opportunity_to_benchmark_75th_percentile",
            "pmr_po_opportunity_to_benchmark_90th_percentile",
            "pmr_po_opportunity_to_benchmark_high",
            "pmr_po_opportunity_to_benchmark_low",
            "pmr_po_opportunity_to_benchmark_median",
            "pmr_po_quantity",
            "pmr_po_quantity_in_eaches",
            "pmr_po_market",
            "pmr_po_acute_or_non_acute",
            "pmr_po_city_geo",
            "pmr_po_state_geo",
            "pmr_po_citystate",
            "pmr_po_zip_geo",
            "pmr_po_county_geo",
            "pmr_po_group",
            "pmr_po_corpcode",
            "pmr_po_bsormh",
            "pmr_po_latitude",
            "pmr_po_longitude",
            "pmr_po_ownership",
            "pmr_po_peer_group",
            "pmr_po_mmis",
            "pmr_po_base_spend_2",
            "pmr_po_mancatnum",
            "pmr_po_proddesc",
            "pmr_po_clean_entity_code",
            "pmr_po_supplierzip",
            "pmr_po_can_acquire_medline_ind",
            "pmr_po_year_month",
            "pmr_po_inv_ln_supplier_invoice_line",
            "pmr_po_inv_ln_market",
            "pmr_po_inv_ln_spend_category",
            "pmr_po_inv_ln_accounting_date",
            "pmr_po_inv_ln_invoice_accounting_date",
            "pmr_po_inv_ln_transaction_date",
            "pmr_po_inv_ln_invoice_status",
            "pmr_po_inv_ln_match_status",
            "pmr_po_inv_ln_payment_status",
            "pmr_po_inv_ln_suppliers_invoice_number",
            "pmr_po_inv_ln_item",
            "pmr_po_inv_ln_line_item_description",
            "pmr_po_inv_ln_supplier_item_catalog_number",
            "pmr_po_inv_ln_quantity",
            "pmr_po_inv_ln_unit_of_measure",
            "pmr_po_inv_ln_unit_cost",
            "pmr_po_inv_ln_extended_amount",
            "pmr_po_inv_ln_manufacturer",
            "pmr_po_inv_ln_manufacture_catalog_number",
            "pmr_if_minority_supplier",
            "pmr_mapping_info",
            "pmr_po_benchmark_facility_count",
            "pmr_po_benchmark_units_purchased",
            "pmr_po_manufacturer_catalog_number_trimmed",
            "pmr_po_uom_1",
            "pmr_po_qoe_1",
            "pmr_po_uom_2",
            "pmr_po_qoe_2",
            "pmr_po_uom_3",
            "pmr_po_qoe_3",
            "pmr_po_uom_4",
            "pmr_po_qoe_4",
            "pmr_po_uom_5",
            "pmr_po_qoe_5",
            "pmr_po_gtin_1",
            "pmr_po_gtin_uom_1",
            "pmr_po_gtin_2",
            "pmr_po_gtin_uom_2",
            "pmr_po_gtin_3",
            "pmr_po_gtin_uom_3",
            "pmr_po_gtin_4",
            "pmr_po_gtin_uom_4",
            "pmr_po_gtin_5",
            "pmr_po_gtin_uom_5"
        ],
        "mapping": {
            "poe_po_unique_id": "poe_po_unique_id",
            "pmr_po_base_each_price": "Base_Each_Price",
            "pmr_po_benchmark_10th_percentile": "BJ_Benchmark_10_Percentile",
            "pmr_po_benchmark_25th_percentile": "BJ_Benchmark_25_Percentile",
            "pmr_po_benchmark_50th_percentile": "BJ_Benchmark_50_Percentile",
            "pmr_po_benchmark_75th_percentile": "BJ_Benchmark_75_Percentile",
            "pmr_po_benchmark_90th_percentile": "BJ_Benchmark_90_Percentile",
            "pmr_po_benchmark_high_price": "BJ_Benchmark_High_Price",
            "pmr_po_benchmark_low_price": "BJ_Benchmark_Low_Price",
            "pmr_po_benchmark_median_price": "BJ_Benchmark_Median_Price",
            "pmr_po_calculated_facility_base_each_price": "Calculated_Facility_Base_Each_Price",
            "pmr_po_calculated_facility_landed_each_price": "Calculated_Facility_Landed_Each_Price",
            "pmr_po_contract_access_price": "Contract_Access_Price",
            "pmr_po_contract_access_tier_description": "Contract_Access_Tier_Description",
            "pmr_po_contract_best_price": "Contract_Best_Price",
            "pmr_po_contract_best_tier_description": "Contract_Best_Tier_Description",
            "pmr_po_contract_category": "Contract_Category",
            "pmr_po_contract_effective_date": "Contract_Effective_Date",
            "pmr_po_contract_expiration_date": "Contract_Expiration_Date",
            "pmr_po_contract_group": "Contract_Group",
            "pmr_po_contract_name": "Contract_Name",
            "pmr_po_contract_number": "Contract_Number",
            "pmr_po_contract_pkg_access_price": "Contract_Pkg_Access_Price",
            "pmr_po_contract_pkg_best_price": "Contract_Pkg_Best_Price",
            "pmr_po_contract_pkg_uom": "Contract_Pkg_UOM",
            "pmr_po_contract_price_found": "Contract_Price_Found",
            "pmr_po_contract_type": "Contract_Type",
            "pmr_po_contract_tagging": "Contract_Utilization_tag",
            "pmr_po_contract_uom_conv": "Contract_UOM_Conv",
            "pmr_po_contracted_catalog_number": "Contracted_Catalog_Number",
            "pmr_po_contracted_supplier": "Contracted_Supplier",
            "pmr_po_contracted_supplier_parent": "Contracted_Supplier_Parent",
            "pmr_po_ctr_flag": "Ctr_Flag",
            "pmr_info": "Pmr_info",
            "pmr_po_data_cleansing_code": "Data_Cleansing_Code",
            "pmr_categorized_spend": "pmr_categorized_spend",
            "pmr_po_direct_parent_entity_code": "Direct_Parent_Entity_Code",
            "pmr_po_direct_parent_name": "Direct_Parent_Name",
            "pmr_po_distributor_markup_percent": "Distributor_Markup_Percent",
            "pmr_po_facility_base_price": "Facility_Base_Price",
            "pmr_po_facility_contract_name": "Facility_Contract_Name",
            "pmr_po_facility_contract_number": "Facility_Contract_Number",
            "pmr_po_facility_contract_price": "Facility_Contract_Price",
            "pmr_po_facility_contract_type": "Facility_Contract_Type",
            "pmr_po_facility_custom_field_1": "Facility_Custom_Field_1",
            "pmr_po_facility_custom_field_2": "Facility_Custom_Field_2",
            "pmr_po_facility_custom_field_3": "Facility_Custom_Field_3",
            "pmr_po_facility_custom_field_4": "Facility_Custom_Field_4",
            "pmr_po_facility_custom_field_5": "Facility_Custom_Field_5",
            "pmr_po_facility_custom_field_6": "Facility_Custom_Field_6",
            "pmr_po_facility_custom_field_7": "Facility_Custom_Field_7",
            "pmr_po_facility_custom_field_8": "Facility_Custom_Field_8",
            "pmr_po_facility_custom_field_9": "Facility_Custom_Field_9",
            "pmr_po_facility_custom_field_10": "Facility_Custom_Field_10",
            "pmr_po_facility_dea": "Facility_DEA",
            "pmr_po_facility_department_name": "Facility_Department_Name",
            "pmr_po_facility_department_number": "Facility_Department_Number",
            "pmr_po_facility_hin": "Facility_Hin",
            "pmr_po_facility_landed_price": "Facility_Landed_Price",
            "pmr_po_facility_manufacturer_catalog_num": "Facility_Manufacturer_Catalog_Num",
            "pmr_po_facility_manufacturer_entity_code": "Facility_Manufacturer_Entity_Code",
            "pmr_po_facility_manufacturer_name": "Facility_Manufacturer_Name",
            "pmr_po_facility_mmis": "Facility_MMIS",
            "pmr_po_facility_name": "Facility_Name",
            "pmr_po_facility_pkg_uom": "Facility_Pkg_UOM",
            "pmr_po_facility_product_description": "Facility_Product_Description",
            "pmr_po_facility_submitted_name": "Facility_Submitted_Name",
            "pmr_po_facility_uom_conv": "Facility_UOM_Conv",
            "pmr_po_facility_vendor_catalog_num": "Facility_Vendor_Catalog_Num",
            "pmr_po_facility_vendor_entity_code": "Facility_Vendor_Entity_Code",
            "pmr_po_facility_vendor_name": "Facility_Vendor_Name",
            "pmr_po_health_system_entity_code": "Health_System_Entity_Code",
            "pmr_po_health_system_name": "Health_System_Name",
            "pmr_po_inner_pack_quantity": "Inner_Pack_Quantity",
            "pmr_po_inner_pack_uom": "Inner_Pack_UOM",
            "pmr_po_landed_each_price": "Landed_Each_Price",
            "pmr_po_manufacturer_catalog_number": "Manufacturer_Catalog_Number",
            "pmr_po_reference_number_surrogate": "Reference_Number_Surrogate",
            "pmr_po_manufacturer_diversity_type": "Manufacturer_Diversity_Type",
            "pmr_po_manufacturer_entity_code": "Manufacturer_Entity_Code",
            "pmr_po_manufacturer_name": "Manufacturer_Name",
            "pmr_po_manufacturer_top_parent_entity_code": "Manufacturer_Top_Parent_Entity_Code",
            "pmr_po_manufacturer_top_parent_name": "Manufacturer_Top_Parent_Name",
            "pmr_po_mdf_date": "MDF_Date",
            "pmr_po_mdf_each_price": "MDF_Each_Price",
            "pmr_po_mdf_found": "MDF_Found",
            "pmr_po_mdf_price": "MDF_Price",
            "pmr_po_mdf_required": "MDF_Required",
            "pmr_po_month": "Month",
            "pmr_po_packaging_string": "Packaging_String",
            "pmr_po_pin": "PIN",
            "pmr_po_pkg_uom": "Pkg_UOM",
            "pmr_po_premier_entity_code": "Premier_Entity_Code",
            "pmr_po_product_description": "Product_Description",
            "pmr_po_program_line": "Program_Line",
            "pmr_po_signed_custom_tier_description": "Signed_Custom_Tier_Description",
            "pmr_po_signed_preferred_tier_indicator": "Signed_Preferred_Tier_Indicator",
            "pmr_po_signed_tier_description": "Signed_Tier_Description",
            "pmr_po_signed_tier_status": "Signed_Tier_Status",
            "pmr_po_signed_tier_status_date": "Signed_Tier_Status_Date",
            "pmr_po_spend_type": "Spend_Type",
            "pmr_po_transaction_date": "Transaction_Date",
            "pmr_po_transaction_line_number": "Transaction_Line_Number",
            "pmr_po_transaction_number": "Transaction_Number",
            "pmr_po_unspsc_class_code": "UNSPSC_Class_Code",
            "pmr_po_unspsc_class_description": "UNSPSC_Class_Description",
            "pmr_po_unspsc_commodity_code": "UNSPSC_Commodity_Code",
            "pmr_po_unspsc_commodity_description": "UNSPSC_Commodity_Description",
            "pmr_po_unspsc_family_code": "UNSPSC_Family_Code",
            "pmr_po_unspsc_family_description": "UNSPSC_Family_Description",
            "pmr_po_unspsc_segment_code": "UNSPSC_Segment_Code",
            "pmr_po_unspsc_segment_description": "UNSPSC_Segment_Description",
            "pmr_po_uom_conv": "UOM_Conv",
            "pmr_po_vendor_entity_code": "Vendor_Entity_Code",
            "pmr_po_vendor_name": "Vendor_Name",
            "pmr_po_vendor_top_parent_entity_code": "Vendor_Top_Parent_Entity_Code",
            "pmr_po_vendor_top_parent_name": "Vendor_Top_Parent_Name",
            "pmr_po_base_spend_1": "Base_Spend",
            "pmr_po_landed_spend": "Landed_Spend",
            "pmr_po_last_base_each_price": "Last_Base_Each_Price",
            "pmr_po_last_base_price": "Last_Base_Price",
            "pmr_po_last_landed_each_price": "Last_Landed_Each_Price",
            "pmr_po_last_landed_price": "Last_Landed_Price",
            "pmr_po_last_transaction_date": "Last_Transaction_Date",
            "pmr_po_opportunity_to_benchmark_10th_percentile": "Opportunity_to_Benchmark_10th_Percentile",
            "pmr_po_opportunity_to_benchmark_25th_percentile": "Opportunity_to_Benchmark_25th_Percentile",
            "pmr_po_opportunity_to_benchmark_50th_percentile": "Opportunity_to_Benchmark_50th_Percentile",
            "pmr_po_opportunity_to_benchmark_75th_percentile": "Opportunity_to_Benchmark_75th_Percentile",
            "pmr_po_opportunity_to_benchmark_90th_percentile": "Opportunity_to_Benchmark_90th_Percentile",
            "pmr_po_opportunity_to_benchmark_high": "Opportunity_To_Benchmark_High",
            "pmr_po_opportunity_to_benchmark_low": "Opportunity_To_Benchmark_Low",
            "pmr_po_opportunity_to_benchmark_median": "Opportunity_To_Benchmark_Median",
            "pmr_po_quantity": "Quantity",
            "pmr_po_quantity_in_eaches": "Quantity_in_Eaches",
            "pmr_po_market": "Market",
            "pmr_po_acute_or_non_acute": "Acute_or_Non_Acute",
            "pmr_po_city_geo": "City_Geo",
            "pmr_po_state_geo": "State_Geo",
            "pmr_po_citystate": "CityState",
            "pmr_po_zip_geo": "Zip_Geo",
            "pmr_po_county_geo": "County_Geo",
            "pmr_po_group": "Group",
            "pmr_po_corpcode": "CorpCode",
            "pmr_po_bsormh": "BSorMH",
            "pmr_po_latitude": "Latitude",
            "pmr_po_longitude": "Longitude",
            "pmr_po_ownership": "Ownership",
            "pmr_po_peer_group": "Peer_Group",
            "pmr_po_mmis": "MMIS",
            "pmr_po_base_spend_2": "Base_Spend<>0",
            "pmr_po_mancatnum": "ManCatNum",
            "pmr_po_proddesc": "ProdDesc",
            "pmr_po_clean_entity_code": "Clean_Entity_Code",
            "pmr_po_supplierzip": "SupplierZip",
            "pmr_po_can_acquire_medline_ind": "Can_acquire_medline_ind",
            "pmr_po_year_month": "Year_Month",
            "pmr_po_inv_ln_supplier_invoice_line": "pmr_po_inv_ln_supplier_invoice_line",
            "pmr_po_inv_ln_market": "pmr_po_inv_ln_market",
            "pmr_po_inv_ln_spend_category": "pmr_po_inv_ln_spend_category",
            "pmr_po_inv_ln_accounting_date": "pmr_po_inv_ln_accounting_date",
            "pmr_po_inv_ln_invoice_accounting_date": "pmr_po_inv_ln_invoice_accounting_date",
            "pmr_po_inv_ln_transaction_date": "pmr_po_inv_ln_transaction_date",
            "pmr_po_inv_ln_invoice_status": "pmr_po_inv_ln_invoice_status",
            "pmr_po_inv_ln_match_status": "pmr_po_inv_ln_match_status",
            "pmr_po_inv_ln_payment_status": "pmr_po_inv_ln_payment_status",
            "pmr_po_inv_ln_suppliers_invoice_number": "pmr_po_inv_ln_suppliers_invoice_number",
            "pmr_po_inv_ln_item": "pmr_po_inv_ln_item",
            "pmr_po_inv_ln_line_item_description": "pmr_po_inv_ln_line_item_description",
            "pmr_po_inv_ln_supplier_item_catalog_number": "pmr_po_inv_ln_supplier_item_catalog_number",
            "pmr_po_inv_ln_quantity": "pmr_po_inv_ln_quantity",
            "pmr_po_inv_ln_unit_of_measure": "pmr_po_inv_ln_unit_of_measure",
            "pmr_po_inv_ln_unit_cost": "pmr_po_inv_ln_unit_cost",
            "pmr_po_inv_ln_extended_amount": "pmr_po_inv_ln_extended_amount",
            "pmr_po_inv_ln_manufacturer": "pmr_po_inv_ln_manufacturer",
            "pmr_po_inv_ln_manufacture_catalog_number": "pmr_po_inv_ln_manufacture_catalog_number",
            "pmr_if_minority_supplier": "pmr_if_minority_supplier",
            "pmr_mapping_info": "pmr_mapping_info",
            "pmr_po_benchmark_facility_count": "pmr_po_benchmark_facility_count",
            "pmr_po_benchmark_units_purchased": "pmr_po_benchmark_units_purchased",
            "pmr_po_manufacturer_catalog_number_trimmed": "pmr_po_manufacturer_catalog_number_trimmed",
            "pmr_po_uom_1": "UOM_1",
            "pmr_po_qoe_1": "UOM_1_QtyEaches",
            "pmr_po_uom_2": "UOM_2",
            "pmr_po_qoe_2": "UOM_2_QtyEaches",
            "pmr_po_uom_3": "UOM_3",
            "pmr_po_qoe_3": "UOM_3_QtyEaches",
            "pmr_po_uom_4": "UOM_4",
            "pmr_po_qoe_4": "UOM_4_QtyEaches",
            "pmr_po_uom_5": "UOM_5",
            "pmr_po_qoe_5": "UOM_5_QtyEaches",
            "pmr_po_gtin_1": "GTIN_1",
            "pmr_po_gtin_uom_1": "GTIN_1_UOM",
            "pmr_po_gtin_2": "GTIN_2",
            "pmr_po_gtin_uom_2": "GTIN_2_UOM",
            "pmr_po_gtin_3": "GTIN_3",
            "pmr_po_gtin_uom_3": "GTIN_3_UOM",
            "pmr_po_gtin_4": "GTIN_4",
            "pmr_po_gtin_uom_4": "GTIN_4_UOM",
            "pmr_po_gtin_5": "GTIN_5",
            "pmr_po_gtin_uom_5": "GTIN_5_UOM"
        },
        "dtype_mapping": {
            "poe_po_unique_id": "string",
            "Base_Each_Price": "double",
            "BJ_Benchmark_10_Percentile": "double",
            "BJ_Benchmark_25_Percentile": "double",
            "BJ_Benchmark_50_Percentile": "double",
            "BJ_Benchmark_75_Percentile": "double",
            "BJ_Benchmark_90_Percentile": "double",
            "BJ_Benchmark_High_Price": "double",
            "BJ_Benchmark_Low_Price": "double",
            "BJ_Benchmark_Median_Price": "double",
            "Calculated_Facility_Base_Each_Price": "double",
            "Calculated_Facility_Landed_Each_Price": "double",
            "Contract_Access_Price": "string",
            "Contract_Access_Tier_Description": "string",
            "Contract_Best_Price": "string",
            "Contract_Best_Tier_Description": "string",
            "Contract_Category": "string",
            "Contract_Effective_Date": "timestamp",
            "Contract_Expiration_Date": "timestamp",
            "Contract_Group": "string",
            "Contract_Name": "string",
            "Contract_Number": "string",
            "Contract_Pkg_Access_Price": "string",
            "Contract_Pkg_Best_Price": "double",
            "Contract_Pkg_UOM": "string",
            "Contract_Price_Found": "string",
            "Contract_Type": "string",
            "Contract_Utilization_tag": "string",
            "Contract_UOM_Conv": "integer",
            "Contracted_Catalog_Number": "string",
            "Contracted_Supplier": "string",
            "Contracted_Supplier_Parent": "string",
            "Ctr_Flag": "string",
            "Pmr_info": "string",
            "Data_Cleansing_Code": "string",
            "pmr_categorized_spend": "string",
            "Direct_Parent_Entity_Code": "string",
            "Direct_Parent_Name": "string",
            "Distributor_Markup_Percent": "string",
            "Facility_Base_Price": "double",
            "Facility_Contract_Name": "string",
            "Facility_Contract_Number": "string",
            "Facility_Contract_Price": "string",
            "Facility_Contract_Type": "string",
            "Facility_Custom_Field_1": "string",
            "Facility_Custom_Field_2": "string",
            "Facility_Custom_Field_3": "string",
            "Facility_Custom_Field_4": "string",
            "Facility_Custom_Field_5": "string",
            "Facility_Custom_Field_6": "string",
            "Facility_Custom_Field_7": "string",
            "Facility_Custom_Field_8": "string",
            "Facility_Custom_Field_9": "string",
            "Facility_Custom_Field_10": "string",
            "Facility_DEA": "string",
            "Facility_Department_Name": "string",
            "Facility_Department_Number": "string",
            "Facility_Hin": "string",
            "Facility_Landed_Price": "double",
            "Facility_Manufacturer_Catalog_Num": "string",
            "Facility_Manufacturer_Entity_Code": "string",
            "Facility_Manufacturer_Name": "string",
            "Facility_MMIS": "string",
            "Facility_Name": "string",
            "Facility_Pkg_UOM": "string",
            "Facility_Product_Description": "string",
            "Facility_Submitted_Name": "string",
            "Facility_UOM_Conv": "integer",
            "Facility_Vendor_Catalog_Num": "string",
            "Facility_Vendor_Entity_Code": "string",
            "Facility_Vendor_Name": "string",
            "Health_System_Entity_Code": "string",
            "Health_System_Name": "string",
            "Inner_Pack_Quantity": "string",
            "Inner_Pack_UOM": "string",
            "Landed_Each_Price": "double",
            "Manufacturer_Catalog_Number": "string",
            "Reference_Number_Surrogate": "string",
            "Manufacturer_Diversity_Type": "string",
            "Manufacturer_Entity_Code": "string",
            "Manufacturer_Name": "string",
            "Manufacturer_Top_Parent_Entity_Code": "string",
            "Manufacturer_Top_Parent_Name": "string",
            "MDF_Date": "integer",
            "MDF_Each_Price": "string",
            "MDF_Found": "string",
            "MDF_Price": "integer",
            "MDF_Required": "string",
            "Month": "string",
            "Packaging_String": "string",
            "PIN": "string",
            "Pkg_UOM": "string",
            "Premier_Entity_Code": "string",
            "Product_Description": "string",
            "Program_Line": "string",
            "Signed_Custom_Tier_Description": "string",
            "Signed_Preferred_Tier_Indicator": "string",
            "Signed_Tier_Description": "string",
            "Signed_Tier_Status": "string",
            "Signed_Tier_Status_Date": "integer",
            "Spend_Type": "string",
            "Transaction_Date": "timestamp",
            "Transaction_Line_Number": "string",
            "Transaction_Number": "string",
            "UNSPSC_Class_Code": "string",
            "UNSPSC_Class_Description": "string",
            "UNSPSC_Commodity_Code": "string",
            "UNSPSC_Commodity_Description": "string",
            "UNSPSC_Family_Code": "string",
            "UNSPSC_Family_Description": "string",
            "UNSPSC_Segment_Code": "string",
            "UNSPSC_Segment_Description": "string",
            "UOM_Conv": "integer",
            "Vendor_Entity_Code": "string",
            "Vendor_Name": "string",
            "Vendor_Top_Parent_Entity_Code": "string",
            "Vendor_Top_Parent_Name": "string",
            "Base_Spend": "double",
            "Landed_Spend": "double",
            "Last_Base_Each_Price": "double",
            "Last_Base_Price": "double",
            "Last_Landed_Each_Price": "double",
            "Last_Landed_Price": "double",
            "Last_Transaction_Date": "timestamp",
            "Opportunity_to_Benchmark_10th_Percentile": "double",
            "Opportunity_to_Benchmark_25th_Percentile": "double",
            "Opportunity_to_Benchmark_50th_Percentile": "double",
            "Opportunity_to_Benchmark_75th_Percentile": "double",
            "Opportunity_to_Benchmark_90th_Percentile": "double",
            "Opportunity_To_Benchmark_High": "double",
            "Opportunity_To_Benchmark_Low": "double",
            "Opportunity_To_Benchmark_Median": "double",
            "Quantity": "double",
            "Quantity_in_Eaches": "double",
            "Market": "string",
            "Acute_or_Non_Acute": "string",
            "City_Geo": "string",
            "State_Geo": "string",
            "CityState": "string",
            "Zip_Geo": "string",
            "County_Geo": "string",
            "Group": "string",
            "CorpCode": "string",
            "BSorMH": "string",
            "Latitude": "string",
            "Longitude": "string",
            "Ownership": "string",
            "Peer_Group": "string",
            "MMIS": "string",
            "Base_Spend<>0": "string",
            "ManCatNum": "string",
            "ProdDesc": "string",
            "Clean_Entity_Code": "string",
            "SupplierZip": "string",
            "Can_acquire_medline_ind": "string",
            "Year_Month": "string",
            "pmr_po_inv_ln_supplier_invoice_line": "string",
            "pmr_po_inv_ln_market": "string",
            "pmr_po_inv_ln_spend_category": "string",
            "pmr_po_inv_ln_accounting_date": "timestamp",
            "pmr_po_inv_ln_invoice_accounting_date": "timestamp",
            "pmr_po_inv_ln_transaction_date": "timestamp",
            "pmr_po_inv_ln_invoice_status": "string",
            "pmr_po_inv_ln_match_status": "string",
            "pmr_po_inv_ln_payment_status": "string",
            "pmr_po_inv_ln_suppliers_invoice_number": "string",
            "pmr_po_inv_ln_item": "string",
            "pmr_po_inv_ln_line_item_description": "string",
            "pmr_po_inv_ln_supplier_item_catalog_number": "string",
            "pmr_po_inv_ln_quantity": "string",
            "pmr_po_inv_ln_unit_of_measure": "string",
            "pmr_po_inv_ln_unit_cost": "string",
            "pmr_po_inv_ln_extended_amount": "string",
            "pmr_po_inv_ln_manufacturer": "string",
            "pmr_po_inv_ln_manufacture_catalog_number": "string",
            "pmr_if_minority_supplier": "string",
            "pmr_mapping_info": "string",
            "pmr_po_benchmark_facility_count": "integer",
            "pmr_po_benchmark_units_purchased": "integer",
            "pmr_po_manufacturer_catalog_number_trimmed": "string",
            "UOM_1": "string",
            "UOM_1_QtyEaches": "string",
            "UOM_2": "string",
            "UOM_2_QtyEaches": "string",
            "UOM_3": "string",
            "UOM_3_QtyEaches": "string",
            "UOM_4": "string",
            "UOM_4_QtyEaches": "string",
            "UOM_5": "string",
            "UOM_5_QtyEaches": "string",
            "GTIN_1": "string",
            "GTIN_1_UOM": "string",
            "GTIN_2": "string",
            "GTIN_2_UOM": "string",
            "GTIN_3": "string",
            "GTIN_3_UOM": "string",
            "GTIN_4": "string",
            "GTIN_4_UOM": "string",
            "GTIN_5": "string",
            "GTIN_5_UOM": "string"
        }
    }
}

df = dyf.toDF()

# Get column order from metadata
order = export_metadata['production_load.l_mt_poe_data']['order']

# Apply column ordering
df = df.select(*order)

# Convert back to DynamicFrame after ordering
dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# Step 2: Rename columns using the mapping
mapping = export_metadata['production_load.l_mt_poe_data']['mapping']
for old, new in mapping.items():
    if old != new and old in df.columns:
        df = df.withColumnRenamed(old, new)

# Convert back to DynamicFrame
pq_df = DynamicFrame.fromDF(df, glueContext, "pq_df")

# Apply data type mapping
dtype_mapping = export_metadata['production_load.l_mt_poe_data']['dtype_mapping']

# 1. Convert DynamicFrame to Spark DataFrame
df = pq_df.toDF()

# 2. Map your dtype strings to Spark SQL types for casting
cast_type_map = {
    "string": StringType(),
    "double": DoubleType(),
    "integer": IntegerType(),
    "timestamp": TimestampType(),
}

# 3. Cast columns to specified types
for col, dtype in dtype_mapping.items():
    if col in df.columns:
        spark_type = cast_type_map.get(dtype, StringType())
        df = df.withColumn(col, df[col].cast(spark_type))

# 4. Convert back to DynamicFrame
pq_df = DynamicFrame.fromDF(df, glueContext, "pq_df")
#this needs to change
def print_schema_markdown(dynamic_frame):
    spark_to_sql = {
        "StringType": "VARCHAR",
        "DoubleType": "DECIMAL(10,4)",
        "IntegerType": "INTEGER",
        "LongType": "BIGINT",
        "TimestampType": "TIMESTAMP",
        "FloatType": "DECIMAL(10,4)",
    }
    df = dynamic_frame.toDF()
    lines = ["| Column Name | Data Type |", "|-------------|-----------|"]
    for field in df.schema.fields:
        spark_type = type(field.dataType).__name__
        sql_type = spark_to_sql.get(spark_type, spark_type)
        lines.append(f"| {field.name} | {sql_type} |")
    return "\n".join(lines)

def parse_markdown_schema(schema_str):
    lines = schema_str.strip().split('\n')
    data_lines = [line for line in lines if '|' in line][2:]  # Skip header and separator
    schema = []
    for line in data_lines:
        parts = [x.strip() for x in line.strip('|').split('|')]
        if len(parts) == 2:
            schema.append(parts)
    return pd.DataFrame(schema, columns=['Column Name', 'Data Type'])

def build_detailed_comparison(expected_df, actual_df):
    actual_cols = list(actual_df['exported_column_name'])
    actual_types = dict(zip(actual_df['exported_column_name'], actual_df['exported_column_datatype']))
    actual_order = {col: idx+1 for idx, col in enumerate(actual_cols)}

    rows = []
    for idx, row in expected_df.iterrows():
        exp_order = int(row['expected_column_order_from_left'])
        exp_col = row['expected_column_name']
        exp_type = row['expected_column_datatype']

        if exp_col in actual_cols:
            act_order = actual_order[exp_col]
            act_col = exp_col
            act_type = actual_types[exp_col]
            order_match = (exp_order == act_order)
            name_match = (exp_col == act_col)
            dtype_match = (exp_type == act_type)
            match_status = "Compatible" if order_match and name_match and dtype_match else "Mismatch"
        else:
            act_order = None
            act_col = None
            act_type = None
            order_match = False
            name_match = False
            dtype_match = False
            match_status = "Missing in Exported"

        rows.append([
            exp_order, exp_col, exp_type,
            act_order, act_col, act_type,
            str(order_match), str(name_match), str(dtype_match), match_status
        ])

    # Also check for extra columns in actual that are not in expected
    for idx, row in actual_df.iterrows():
        act_col = row['exported_column_name']
        if act_col not in list(expected_df['expected_column_name']):
            act_order = idx + 1
            act_type = row['exported_column_datatype']
            rows.append([
                None, None, None,
                act_order, act_col, act_type,
                "False", "False", "False", "Extra in Exported"
            ])

    columns = [
        "expected_column_order_from_left", "expected_column_name", "expected_column_datatype",
        "exported_column_order_from_left", "exported_column_name", "exported_column_datatype",
        "Column Order Match", "Column Name Match", "Column Datatype Match", "match_status"
    ]
    return pd.DataFrame(rows, columns=columns)

def send_html_email(subject, body, to_email, from_email):
    try:
        ses_client = boto3.client('ses', region_name='us-east-1')
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = to_email
        html_part = MIMEText(body, 'html')
        msg.attach(html_part)
        message_body = msg.as_string()
        response = ses_client.send_raw_email(
            Source=from_email,
            Destinations=[to_email],
            RawMessage={'Data': message_body}
        )
        return response
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return None

# Load predefined schema from S3
predefined_schema = pd.read_csv('s3://dbt-us-east-2-ohio-v2/POE/predefined_schema/Predefined_schema.csv', dtype=str)

# Get actual schema from processed DynamicFrame
schema_str = print_schema_markdown(pq_df)
actual_schema = parse_markdown_schema(schema_str)
actual_schema = actual_schema.rename(columns={
    'Column Name': 'exported_column_name',
    'Data Type': 'exported_column_datatype'
})

# Build detailed comparison DataFrame
detailed_comparison = build_detailed_comparison(predefined_schema, actual_schema)

# Sort by expected_column_order_from_left (metadata order), extra columns at the end
detailed_comparison = detailed_comparison.sort_values(
    by=["expected_column_order_from_left", "exported_column_order_from_left"],
    na_position='last'
)

# Convert to HTML
comparison_html = detailed_comparison.to_html(index=False, border=1)

# Create email body with table
email_body = f"""
<html>
<head>
<style>
    body {{ font-family: Arial, sans-serif; }}
    .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; }}
    th {{ background-color: #f2f2f2; }}
</style>
</head>
<body>
    <div class="header">
        <h2>Schema Comparison Report</h2>
        <p>Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    <p>Dear Team,</p>
    <p>Please find below the detailed schema comparison report for the Glue DynamicFrame job.</p>
    <h3>Detailed Schema Comparison:</h3>
    {comparison_html}
    <p>Regards,<br>SupplyCopia ETL Team</p>
</body>
</html>
"""

# Send email without attachment, preserving order
try:
    send_html_email(
        subject="Glue DynamicFrame Schema Comparison Report",
        body=email_body,
        to_email=[
        "zaziz@supplycopia.com",
        "rhansraj@supplycopia.com",
        "ryadav@supplycopia.com"
    ]
        from_email="SupplyCopia <no-reply@supplycopia.com>"
    )
    print("Email sent successfully (no attachment)")
except Exception as e:
    print(f"Error sending email: {str(e)}") 

final_df = pq_df.toDF()

sorted_df = final_df.orderby("Contract_Effective_Date")

s3_output_path = "s3://dbt-us-east-2-ohio-v2/POE/final_file/"

# Set the config to handle ancient dates
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

# Repartition by partition column to get one file per partition value
sorted_df = sorted_df.repartition(4)

sorted_df.write.mode("overwrite").partitionBy("pmr_po_month").parquet(s3_output_path)
print(f"Data written to {s3_output_path} in Parquet format, partitioned by pmr_po_month.")