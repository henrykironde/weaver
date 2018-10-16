from __future__ import print_function

from collections import OrderedDict


def make_sql(dataset):
    # 1. For spatial joins,
    # assume that the latitude and longitude fields are in the main file.
    # use ST_PointFromText ST_PointFromText('POINT(-71.064 42.28)', 4326);
    # 2. Refactoring to after raster joins are finalized

    main_table_path = dataset.main_file["path"]
    as_processed_table = OrderedDict([(main_table_path, "T1")])
    query_statement = ""
    all_fields = []
    unique_f = set()
    make_temp = False

    if "fields" in dataset.main_file:
        if dataset.main_file["fields"]:
            all_fields += [as_processed_table[main_table_path] + "." + item_f
                          for item_f in dataset.main_file["fields"]]
            unique_f |= set(dataset.main_file["fields"])

    # Process all tables that are to be joined
    # and the fields that are required

    for count, dict_joins in enumerate(dataset.join):
        table2join =OrderedDict()
        table2join=dict_joins

        as_tables = "as_" + str(count)  # Table references Table as T
        as_tables_dot = as_tables + "."
        as_processed_table[table2join["table"]] = as_tables

        # Create the values for the final select of fields using the
        # references Table name. Select a.t1, b.t1 c.t2 From ..
        # ie. a.t1, b.t1 c.t2
        fields_used = []
        if "fields_to_use" in table2join:
            if table2join["fields_to_use"]:
                fields_used = table2join["fields_to_use"]
            elif table2join["table_type"] == "raster":
                # use all the fields in tables[fields]
                for table_dict in dataset.tables:
                    db_table_name= table_dict['database_name'] + '.' + table_dict['table_name']
                    if db_table_name == table2join['table']:
                        fields_used = table_dict['fields']
                        continue

        all_fields += [as_tables_dot + field_name for field_name in fields_used if field_name  not in unique_f]
        unique_f |= set(fields_used)
        fields_string = ', '.join(str(e) for e in fields_used)

        if "table_type" in table2join:
            # use the longitude ()and latitude to calculate the the_geom in a temp table
            # Note x is longitude and y is latitude
            # the_geom = ST_MakePoint(double precision x, double precision y) or
            # the_geom = ST_PointFromText('POINT(-71.064544 42.28787)', 4326);
            if table2join["table_type"] == "vector":
                make_temp = True  # Used to create a temp table with a geom
                left_join = "\nLEFT JOIN {table_i} {tablei_as} " \
                            "\nON ST_Within(t1.geom, {tablei_as}.geom) " \
                            "\n".format(table_i=table2join["table"], tablei_as=as_tables)
                query_statement += left_join
            elif table2join["table_type"] == "raster":
                # if fields_string.strip() == "":
                #     fields_string +=  " rast As rast "
                # else:
                #     fields_string += ", rast As rast "
                make_temp = True
                # ["latitude", "longitude"] [y,x]
                y = dataset.result["lat_long"][0]
                x = dataset.result["lat_long"][1]
                left_join = "\nLEFT OUTER JOIN " \
                            "\n\t(SELECT {fields_used} " \
                            "\n\tFROM {table_j}) AS {table_j_as} " \
                            "".format(table_j=table2join["table"],
                                      table_j_as=as_tables,
                                      fields_used=fields_string)
                left_join += "\nON ST_Intersects(" + as_tables + ".rast, ST_PointFromText(FORMAT('POINT(%s %s)', cast("+ x + " as varchar), cast("+ y +" as varchar)), 4326))\n"

                # left_join += where_sql
                query_statement += left_join
            else:
                # "table_type" is tabular
                where_clause = ""
                if "lat_long" in table2join:
                    # ["latitude", "longitude"] [y,x]
                    y = table2join["lat_long"][0]
                    x = table2join["lat_long"][1]
                    where_clause = "WHERE {latitude} Not LIKE '%NULL%' AND {longitude} Not LIKE '%NULL%' ".format(latitude=y, longitude=x)

                left_join = "\nLEFT OUTER JOIN " \
                            "\n\t(SELECT {fields_used} " \
                            "\n\tFROM {table_j} " \
                            "\n\t{where_st}) AS {table_j_as} " \
                            "".format(table_j=table2join["table"],
                                      table_j_as=as_tables,
                                      where_st=where_clause,
                                      fields_used=fields_string)
                left_join += "\nON \n"

                # Process "ON" statements, having either similar
                # or different field/column names
                on_common_stmt = []
                if table2join["join_ocn"]["common_field"]:
                    for item_field in table2join["join_ocn"]["common_field"]:
                        on_common_stmt += ["{table_i}.{common_name}={table_j}.{common_name}".format(
                            table_i=as_processed_table[dataset.main_file["path"]],
                            table_j=as_processed_table[table2join["table"]],
                            common_name=str(item_field))]

                # process non common fields
                temp_dict = table2join["join_ocn"]
                temp_dict.pop("common_field", True)

                # Two lists whose index each match the field to use
                # when creating the conditional statement
                # Use index of one to obtain the value of the other
                tab_field_index = list(temp_dict.keys())
                on_diff_stmt = []
                for num, items in enumerate(temp_dict[tab_field_index[0]]):
                    new_on = "{tab_i}.{field_i}={tab_j}.{field_j}".format(
                        tab_i=as_processed_table[str(tab_field_index[0])],
                        # field_i=items,
                        field_i= temp_dict[tab_field_index[0]][num],
                        tab_j=as_processed_table[str(tab_field_index[1])],
                        field_j=temp_dict[tab_field_index[1]][num])
                    on_diff_stmt.append(new_on)

                all_on_stmts = on_diff_stmt + on_common_stmt

                on_condition = " AND ".join(str(etr) for etr in all_on_stmts)
                left_join += on_condition
                query_statement += left_join

    # Process the main file and create the query string for all the required fields
    if "fields" in dataset.main_file:
        where_clause = ""
        if "lat_long" in dataset.main_file and dataset.main_file["lat_long"]:
            # ["latitude", "longitude"] [y,x]
            y = table2join["lat_long"][0]
            x = table2join["lat_long"][1]
            where_clause = "\nWHERE {latitude} Not LIKE '%NULL%' AND {longitude} Not LIKE '%NULL%' ".format(
                latitude=y, longitude=x)

        pivot_query = "\nSELECT {all_fls} into {res} " \
                      "\nFROM {main_table} {where_stm} AS {table_m} " \
                      "".format(all_fls=', '.join(str(e) for e in all_fields),
                                main_table=dataset.main_file["path"],
                                res="{result_dbi}.{result_tablei}",
                                where_stm= where_clause,
                                table_m=as_processed_table[main_table_path])
        if make_temp:
            # ["latitude", "longitude"] [y,x]
            y = dataset.result["lat_long"][0]
            x = dataset.result["lat_long"][1]

            temp_fields = ["temp." + item_k for item_k in dataset.main_file["fields"]]
            temp_fields_string = ', '.join(str(e) for e in temp_fields) + ", "

            temp_geom_value = temp_fields_string + "ST_PointFromText('POINT(" \
                                                   "cast(temp.{x1} as varchar) " \
                                                   "cast(temp.{y1} as varchar))', " \
                                                   "4326) as the_geom ".format(x1=x, y1=y)
            pivot_query = "\nSELECT {all_flds} " \
                          "\nINTO {res} " \
                          "\nFROM (SELECT  {temp_geom_value}  " \
                          "\nFROM {main_table} temp " \
                          "\n{where_stm}) {table_m} ".format(
                all_flds=', '.join(str(e) for e in all_fields),
                temp_geom_value=temp_geom_value,
                main_table=dataset.main_file["path"],
                res="{result_dbi}.{result_tablei}",
                table_m=as_processed_table[main_table_path],
                where_stm=where_clause)

    print(pivot_query+ query_statement)
    exit()
    # return pivot_query+ query_statement


