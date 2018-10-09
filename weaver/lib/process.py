from __future__ import print_function

from collections import OrderedDict
# from weaver.lib.models import *


class Processor(object):
    def make_sql(dataset):
        # 1. For spatial joins,
        # assume that the latitude and longitude fields are in the main file.
        # use ST_PointFromText ST_PointFromText('POINT(-71.064 42.28)', 4326);

        dataset

        main_table_path = dataset.main_file["path"]
        as_processed_table = OrderedDict([(main_table_path, "T1")])
        as_processed_table[dataset.main_file["path"]] = "t1"
        main_sql_join = ""
        all_fields = []
        all_field_string = ""
        make_temp = False

        # Process all tables that are to be joined and the fields that are required
        for counter, tabletojoin in enumerate(dataset.join):
            as_tables = "as_" + str(counter)
            dot_tablevalue = as_tables + "."
            as_processed_table[tabletojoin["table"]] = as_tables

            # Obtain all the values for the final select of fields
            # Select a.t1, b.t1 c.t2 From ..
            # ie. a.t1, b.t1 c.t2
            f_fields_used = []
            if "fields_to_use" in tabletojoin:
                if tabletojoin["fields_to_use"]:
                    all_fields += [dot_tablevalue + item_k for item_k in tabletojoin["fields_to_use"]]
                    f_fields_used = tabletojoin["fields_to_use"]
            else:
                # Use valuse of fields in tables or *, for now lets use *
                # To dos
                f_fields_used = [" _*_ "]
            str_f_fields_used = ', '.join(str(e) for e in f_fields_used)

            if "table_type" in tabletojoin:
                # use the longitude ()and latitude to calculate the the_geom in a temp table
                # Note x is longitude and y is latitude
                # the_geom = ST_MakePoint(double precision x, double precision y) or
                # the_geom = ST_PointFromText('POINT(-71.064544 42.28787)', 4326);
                if tabletojoin["table_type"] == "vector":
                    make_temp = True
                    left_join = "LEFT JOIN {table_i} {tablei_as} " \
                                "ON ST_Within(t1.geom, {tablei_as}.geom)".format(table_i=tabletojoin["table"], tablei_as=as_tables)
                    main_sql_join += left_join
                elif tabletojoin["table_type"] == "raster":
                    pass
                else:
                    # "table_type" is tabular
                    left_join = "\n\nLEFT OUTER JOIN \n(SELECT {fields_used} \n\tFROM {t2_j}) AS {tsb} ".format(
                        t2_j=tabletojoin["table"],
                        tsb=as_tables,
                        fields_used=str_f_fields_used)
                    left_join += "\nON \n"
                    string_b4_list = []
                    if tabletojoin["join_ocn"]["common_field"]:
                        for item_field in tabletojoin["join_ocn"]["common_field"]:
                            string_b4_list += ["{t11}.{samefield}={t22}.{samefield}".format(
                                t11=as_processed_table[dataset.main_file["path"]],
                                t22=as_processed_table[tabletojoin["table"]],
                                samefield=str(item_field))]
                    # process non common fields
                    temp_dict = tabletojoin["join_ocn"]
                    temp_dict.pop("common_field", True)

                    # list of tables names:
                    lll = list(temp_dict.keys())
                    string_3_on_list = []
                    for counter, items in enumerate(temp_dict[lll[0]]):
                        new_on = "{tt1}.{f11}={tt2}.{f22}".format(
                            tt1=as_processed_table[str(lll[0])], f11=items, tt2=as_processed_table[str(lll[1])], f22=temp_dict[lll[0]][counter] )
                        string_3_on_list.append(new_on)


                    all_onz = string_3_on_list + string_b4_list
                    lllllllstr_2_j_on = " AND ".join(str(etr) for etr in all_onz)
                    left_join += lllllllstr_2_j_on
                    main_sql_join += left_join


        # Process the main file and create the query string for all the required fields
        if "fields" in dataset.main_file:
            if not dataset.main_file["fields"]:
                bmain_sql_join = "\nSELECT {al} into {res}\n  FROM {main_table}  AS  t1 " \
                                 "".format(main_table=dataset.main_file["path"],
                                           al=', '.join(str(e) for e in all_fields),
                                           res="{result_dbi}.{result_tablei}")
            else:
                all_fields = dataset.main_file["fields"]
                all_fields = ["t1." + itemkk for itemkk in dataset.main_file["fields"]]
                bmain_sql_join = "\nSELECT {all_fls} into {res} \nFROM {main_table} AS t1 " \
                                 "".format(all_fls="{all_flds}", main_table=dataset.main_file["path"],
                                           res="{result_dbi}.{result_tablei}")
                if make_temp:
                    # ["latitude", "longitude"]
                    y = dataset.main_file["lat_long"][0]
                    x = dataset.main_file["lat_long"][1]

                    temp_fields = ["temp." + itemkk for itemkk in dataset.main_file["fields"]]
                    temp_fields_string = ', '.join(str(e) for e in temp_fields) + ", "
                    temp_geom_value = temp_fields_string + " (ST_SetSRID(ST_MakePoint(cast(temp.{x1} as Numeric(10, 4)), cast(temp.{y1} as Numeric(10, 4))), 4326)) as the_geom".format(
                        x1=x, y1=y)
                    bmain_sql_join = "\nSELECT * FROM (SELECT " + temp_geom_value + " FROM {main_table} temp) t1"
                    bmain_sql_join = "\nSELECT {all_fls} into {res} FROM (SELECT" + temp_geom_value + " FROM {main_table} temp) t1".\
                        format(all_fls="{all_flds}", main_table=dataset.main_file["path"],
                                           res="{result_dbi}.{result_tablei}")

        str_allfields = ', '.join(str(e) for e in all_fields)
        # print(bmain_sql_join)
        m = main_sql_join.format(all_flds=str_allfields)



        return bmain_sql_join +" "+ m
