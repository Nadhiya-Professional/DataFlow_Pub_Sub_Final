
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from apache_beam.io.gcp.internal.clients import bigquery


# This Class Transforms the Customer full name into first name and last name

class Parse_Name(beam.DoFn):

    def process(self, element):
        data_dict = json.loads(element.decode('utf-8'))
        new_dict = {}
        new_dict["customer_firstname"] = data_dict["customer_name"].split(" ")[0]
        new_dict["customer_lastname"] = data_dict["customer_name"].split(" ")[1]
        yield new_dict


# This Class is for Parsing Address. There was no uniformity in the addresses. So used different if conditions to put
# some values in the columns. There were four different formats of addresses.
# 1 - Unit 4658 Box 0910, DPO AA 80360
# 2 - USNS Dominguez, FPO AP 69504
# 3 - PSC 9895, Box 3222, APO AE 30642
# 4 - 5831 Jones Fords, Dorisbury, CT 52999

class Parse_Address(beam.DoFn):

    def process(self, element):
        element1 = {}
        order_address_split = element["order_address"].split(",")
        if len(order_address_split) > 2:
            building_and_street = order_address_split[0]
            state_and_zip = order_address_split[2]
            order_city = order_address_split[1].strip()

            if len(state_and_zip.split(" ")) == 3:
                zip_code = state_and_zip.split(" ")[2]
                state_code = state_and_zip.split(" ")[1]

            if len(state_and_zip.split(" ")) > 3:
                zip_code = state_and_zip.split(" ")[3]
                state_code = state_and_zip.split(" ")[2]
                order_city = order_address_split[1] + " " + state_and_zip.split(" ")[1]
            if building_and_street.split()[0].isnumeric():
                building_no = int(building_and_street.split()[0])
                street_name = building_and_street.split(" ", 1)[1]
            else:
                building_no = int(building_and_street.split()[1].strip())
                street_name = order_city + building_and_street.split()[0]

            element1["order_building_no"] = building_no
            element1["order_street_name"] = street_name
            element1["order_city"] = order_city
            element1["order_state_code"] = state_code
            element1["order_zip_code"] = zip_code
            element["order_address1"] = [element1]

        else:
            building_no = int()
            street_name = order_address_split[0]
            city_state_zip = order_address_split[1]
            order_city = city_state_zip.split()[0].strip()
            state_code = city_state_zip.split()[1].strip()
            zip_code = city_state_zip.split()[2].strip()
            element1["order_building_no"] = building_no
            element1["order_street_name"] = street_name
            element1["order_city"] = order_city
            element1["order_state_code"] = state_code
            element1["order_zip_code"] = zip_code

            element["order_address1"] = [element1]

        yield element


# This Class is used to calculate the total cost

class Calculate_Amount(beam.DoFn):

    def process(self, element):
        total_sum = 0.0
        if element["order_items"]:
            for item in element["order_items"]:
                total_sum += item["price"]
        total_sum += element["cost_tax"] + element["cost_shipping"]
        element["cost_total"] = round(total_sum,2)
        yield element

# This Class filters just the required columns for the pub/sub data.


class Filter_order(beam.DoFn):
    def process(self, element):
        columns_to_extract = ["order_id", "order_items"]
        new_set = {k: element[k] for k in columns_to_extract}
        yield new_set

# Extracts just the item_id from the order_items, name and price are rejected.


class Order_Seg(beam.DoFn):
    def process(self, element):
        list_id = []
        for item in element["order_items"]:
            list_id.append(item["id"])
        element["order_id"] = element["order_id"]
        element["items_list"] = list_id
        del element["order_items"]
        yield element


# Extracts just the USD Rows

def IS_USD(element1):
    return element1["order_currency"] == "USD"

# Extracts just the EUR Rows


def IS_EUR(element2):
    return element2["order_currency"] == "EUR"

# Extracts just the GBP Rows


def IS_GBP(element3):
    return element3["order_currency"] == "GBP"

# Extracts just the required Columns to put the result in the Bigquery tables


class Filter_Data(beam.DoFn):
    def process(self, data):
        columns_to_extract = ["order_id", "order_address1", "customer_firstname", "customer_lastname", "customer_ip",
                              "cost_total"]
        new_set = {k: data[k] for k in columns_to_extract}
        yield new_set


if __name__ == '__main__':
    # Schema definition for all the three tables

    table_schema = {
        'fields': [
#             {'name': 'order_id', 'type': 'INTEGER', 'mode': 'nullable'},
#             {"name": "order_address1", "type": "RECORD", 'mode': 'Repeated',
#              'fields': [
#                  {"name": "order_building_no", "type": "Integer", 'mode': 'Nullable'},
#                  {"name": "order_street_name", "type": "STRING", 'mode': "Nullable"},
#                  {"name": "order_city", "type": "STRING", 'mode': 'NULLABLE'},
#                  {"name": "order_state_code", "type": "STRING", 'mode': 'NULLABLE'},
#                  {"name": "order_zip_code", "type": "Integer", 'mode': 'NULLABLE'},
#              ],
#              },
            {"name": "customer_firstname", "type": "STRING", 'mode': 'NULLABLE'},
            {"name": "customer_lastname", "type": "STRING", 'mode': 'NULLABLE'}
#             {'name': 'customer_ip', 'type': 'String', 'mode': 'nullable'},
#             {"name": "cost_total", "type": "Float", 'mode': 'NULLABLE'}
        ]
    }
    # Table specifications for the first BigQuery Table

    table_spec1 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='n_mathialagan_proj_1',
        tableId='data_flow')

    # Table specifications for the second  BigQuery Table

#     table_spec2 = bigquery.TableReference(
#         projectId='york-cdf-start',
#         datasetId='n_mathialagan_proj_1',
#         tableId='table12')

#     # Table specifications for the third Bigquery table

#     table_spec3 = bigquery.TableReference(
#         projectId='york-cdf-start',
#         datasetId='n_mathialagan_proj_1',
#         tableId='table13')

    pipeline_options = PipelineOptions(streaming=True, save_main_session=True)
    argv1 = ["project","york-cdf-start", "region","uscentral-1","runner","DataflowRunner", "temp_location","gs://nm_york_cdf-_start/results/tmp/","staging_location","gs://nm_york_cdf-_start/stage"]

    with beam.Pipeline(options=pipeline_options,argv=argv1) as pipeline:
        # Entire data being pulled from the Pub/Sub subscription.

        entire_data = pipeline | beam.io.ReadFromPubSub(
            subscription="projects/york-cdf-start/subscriptions/Nadhiya-NM-sub-new")

        # Splitting the customer name into first and last name
        names = entire_data | beam.ParDo(Parse_Name())
        
        # Splitting the address

#         address = names | beam.ParDo(Parse_Address())

#         # Calculating total price by joining tax, shipping and price

#         total_price = address | beam.ParDo(Calculate_Amount())

#         # Filters and rearranges the order_items to write in the pub/sub

#         order_separation = total_price | beam.ParDo(Filter_order())

#         order_segregation = order_separation | beam.ParDo(Order_Seg())

#         # Filters just the USD data

#         dictionary_separation_1 = total_price | beam.Filter(IS_USD)

#         # Filters just the EURO data

#         dictionary_separation_2 = total_price | beam.Filter(IS_EUR)

#         # Filters just the GBP data
#         dictionary_separation_3 = total_price | beam.Filter(IS_GBP)

#         # Filters just the required columns

#         US_order = dictionary_separation_1 | "Filter_For_USD" >> beam.ParDo(Filter_Data())
#         EU_order = dictionary_separation_2 | "Filter_For_EUR" >> beam.ParDo(Filter_Data())
#         GBP_order = dictionary_separation_3 | "Filter_For_GBP" >> beam.ParDo(Filter_Data())

#         # Writing to the first big query table

        names | "write1" >> beam.io.WriteToBigQuery(
            table_spec1,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

#         # Writing to the second big query table

#         EU_order | "write2" >> beam.io.WriteToBigQuery(
#             table_spec2,
#             schema=table_schema,
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

#         # Writing to the third big query table

#         GBP_order | "write3" >> beam.io.WriteToBigQuery(
#             table_spec3,
#             schema=table_schema,
#             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

#         # To convert the PCollection to bytecode, first I am converting the PCollection to dictionary and to string
#         # and then encoding to byte codes.

#         order_final = order_segregation | beam.Map(lambda s: json.dumps(s).encode("utf-8"))
#         order_final | "Write to PubSub" >> beam.io.WriteToPubSub(topic="projects/york-cdf-start/topics/dataflow-order"
#                                                                        "-stock-update",
#                                                                  with_attributes=False)
        
        pipeline_result = pipeline.run()
        pipeline_result.wait_until_finish(duration=100000)
        pipeline_result.cancel()
