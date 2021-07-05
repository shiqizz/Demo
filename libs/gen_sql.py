__author__ = 'Russell'

import re
import hashlib
import itertools
import collections
from pathlib import Path

import pandas as pd


class SQLGenerator:
    def __init__(self, is_and=False, where_in_value=False):
        self.is_and = is_and
        self.where_in_value = where_in_value

        self.table_key = 'main_chart'
        self.index_temp_key = 'index_name'
        self.formula_key = 'formular'
        self.cal_property_key = 'variable'
        self.conditions_key = 'restrict'
        self.group_key = 'polymeric'
        self.date_key = 'date_field'
        self.area_key = 'area'
        self.on_table_key = 'related_chart'
        self.on_fields_key = 'related_field'
        self.detail_fields_key = 'details_field'
        self.proportion_key = 'denominator'
        self.distinct_key = 'remove_repeat'
        self.detail_orders_key = 'order_by'
        self.detail_order_type_key = 'orderby_type'
        self.group_conditions_key = 'polymeric_list'
        self.dynamic_conditions_key = 'dynamic_select'
        self.multiple_options_key = 'multiple_choice'
        self.time_unit_key = 'time_unit'

        self.raw_field2value_key = 'raw_field2value'
        self.second_formula_key = 'second_formula'
        self.dynamic_select_info = 'dynamic_select_info'

        self.auto2field = {
            'rule_id': self.index_temp_key,
            'product_code': 'product_name'
        }
        self.system_time = 'system_time'
        self.vertical_pat = re.compile(r'\s*[|]\s*')
        self.default_null = 'NUR'
        self.is_exist = 'is_exist'
        self.sys_update_date = 'sys_insert_date'

        self.nested_pat = re.compile(
            r'([A-Z]\.{0}+?)\.({0}+)'.format('[a-zA-Z_0-9]')
        )

    def excel_to_sql(self, excel_path=None):
        # Audit parameters
        # excel_path = Path(excel_path or 'data/sfz/指标规则#20200604.xlsx')
        excel_path = Path(
            excel_path or 'data/sfz/inx_regular_program.csv'
        )

        # Loading Excel to DataFrame
        if '.xls' in excel_path.suffix:
            df = pd.read_excel(
                open(excel_path.as_posix().encode('utf_8'), 'rb'),
                skiprows=[0, 1, 2], dtype=str
            )
        # Loading CSV to DataFrame
        elif '.csv' in excel_path.suffix:
            df = pd.read_csv(
                open(excel_path.as_posix().encode('utf_8'), 'rb'),
                dtype=str
            )
        else:
            raise TypeError('Only Support Excel and CSV file')
        old_len = len(df)
        df.dropna(subset=[self.table_key], inplace=True)
        print('There are {} NULL records'.format(old_len - len(df)))
        df.fillna('', inplace=True)

        # Generating SQL
        sql_list = []
        for _, key2value in df.iterrows():
            sql_list.append(self.conf_to_sql(key2value, is_debug=True))
        for sql in sql_list:
            print(sql)

    def conf_to_sql(
            self, key2value: dict,
            exec_sql=None, is_debug=False, is_auto=False
    ):
        # Auditing parameters
        key2value = {
            k_: '' if v_ is None else v_ for k_, v_ in key2value.items()
        }

        # Setting variables
        multi_fields = {
            self.conditions_key, self.group_key,
            self.on_fields_key, self.detail_fields_key,
            self.detail_orders_key, self.group_conditions_key,
            self.dynamic_conditions_key, self.multiple_options_key
        }
        not_raw_fields = {
            self.table_key, self.index_temp_key, self.formula_key,
            self.cal_property_key, self.conditions_key, self.group_key,
            self.date_key, self.area_key, self.on_table_key,
            self.on_fields_key, self.detail_fields_key,
            self.proportion_key, self.distinct_key, self.time_unit_key,
            self.detail_order_type_key, 'create_date', 'update_date'
        }

        # Extracting index configurations
        key2field = {self.raw_field2value_key: dict()}
        for row_field, value in key2value.items():
            if row_field in multi_fields:
                key2field[row_field] = [
                    field_.strip()
                    for field_ in self.vertical_pat.split(value)
                    if field_.strip()
                ] if value else []
            elif row_field in not_raw_fields:
                key2field[row_field] = value
            else:
                key2field[self.raw_field2value_key][row_field] = value
        (
            key2field[self.formula_key], key2field[self.second_formula_key]
        ) = (self.vertical_pat.split(key2value[self.formula_key]) + [''])[:2]

        # Generating field values using MD5
        if is_auto:
            for auto, field in self.auto2field.items():
                key2field[self.raw_field2value_key][auto] = hashlib.md5(
                    key2value[field].encode('utf_8')
                ).hexdigest()

        return self.js_to_sql(key2field, exec_sql, is_debug)

    def js_to_sql(self, key2field: dict, exec_sql, is_debug=False):
        """
        Generating SQL

        :param key2field:
        :type key2field:
        :param exec_sql:
        :type exec_sql:
        :param is_debug:
        :type is_debug:
        :return:
        :rtype:

        >>> self.js_to_sql({
            self.table_key: 'ods_company',
            self.index_temp_key: '高精尖企业数：A.company_type：占比',
            self.formula_key: 'count',
            self.cal_property_key: '',
            self.conditions_key: ["A.is_excellent='1'"],
            self.group_key: [
                'A.field', 'A.company_type'
            ],
            self.date_key: 'system_time',
            self.detail_fields_key: [
                'A.company_name', 'A.field',
                'A.company_type', 'A.address_area'
            ],
            self.raw_field2value_key: {
                'unit': '家',
                'product_name': '重点企业数量'
            }
        })

        >>> self.js_to_sql({
            self.table_key: 'ods_company',
            self.index_temp_key: '高精尖上市企业市值：A.address_area',
            self.formula_key: 'sum',
            self.cal_property_key: 'B.value',
            self.group_key: [
                'A.field', 'A.address_area'
            ],
            self.date_key: 'system_time',
            self.on_table_key: 'ods_market_value',
            self.on_fields_key: ['A.code_id=B.code_id'],
            self.detail_fields_key: [
                'A.company_name', 'A.field',
                'A.company_type', 'A.address_area', 'B.value'
            ],
            self.raw_field2value_key: {
                'unit': '亿元',
                'product_name': '重点企业营业收入、研发投入'
            }
        })

        >>> self.js_to_sql({
            self.table_key: 'ods_excellent_company_business_index',
            self.index_temp_key: '高精尖财务营业收入总额：同比 ',
            self.formula_key: 'sum',
            self.second_formula_key: '同比',
            self.cal_property_key: 'A.sales/10000',
            self.group_key: [
                'A.date'
            ],
            self.date_key: 'A.date',
            self.detail_fields_key: [
                'A.company_name', 'A.field',
                'A.company_type'
            ],
            self.raw_field2value_key: {
                'unit': '%',
                'product_name': '重点企业营业收入、研发投入'
            }
        })
        """
        # Appending order by fields
        key2field[self.detail_fields_key].extend(
            set(key2field[self.detail_orders_key]).difference(
                key2field[self.detail_fields_key]
            )
        )

        # From clause
        from_table = '{} A'.format(key2field[self.table_key])
        # On clause
        on_info = ' LEFT JOIN {} B'.format(
            key2field[self.on_table_key]
        ) if key2field.get(self.on_table_key) else ''
        on_info += ' ON {}'.format(
            ' AND '.join(key2field[self.on_fields_key])
        ) if key2field.get(self.on_table_key) else ''
        # Where clause
        exist_conditions = []
        table_shorts = [
            'A', *(['B'] if key2field.get(self.on_table_key) else [])
        ]
        for table_short in table_shorts:
            exist_conditions.extend([
                '{}.{}=1'.format(table_short, self.is_exist),
                '{}.{} <= now()'.format(table_short, self.sys_update_date)
            ])
        if key2field.get(self.conditions_key):
            # Only for = and in condition
            pat = re.compile(
                r'([A-Z]\.{0}+?)\.({0}+)\s*(=| in )\s*(.+)'.format(
                    '[a-zA-Z_0-9]'
                )
            )
            pat_sub = re.compile(r"^\s*'|'\s*$")
            js_conditions = []
            for condition in key2field[self.conditions_key]:
                mat = pat.fullmatch(condition)
                if mat:
                    if mat.group(3) == '=':
                        values = [mat.group(4)]
                    else:
                        values = re.split(
                            r'\s*,\s*',
                            re.sub(r'^\s*\(|\)\s*$', '', mat.group(4))
                        )
                    js_conditions.append(
                        '(' + ' OR '.join(
                            "{field} @> '[{left}{mark}{key}{mark}: "
                            "{value}{right}]'".format(
                                field=mat.group(1), key=mat.group(2),
                                value=pat_sub.sub('"', value_),
                                left='{', right='}', mark='"'
                            )
                            for value_ in values
                        ) + ')'
                    )
            where_info = ' WHERE ' + ' AND '.join([
                *exist_conditions,
                *js_conditions, *key2field[self.conditions_key]
            ])
        else:
            where_info = ' WHERE ' + ' AND '.join(exist_conditions)

        # Date unit
        time_unit = (key2field.get(self.time_unit_key) or '').strip()
        if time_unit == '':
            start_time_format = '{}'
            time_format = '{}'
        elif time_unit in ['年', 'year', 'years']:
            start_time_format = "date_trunc('year', {})::date"
            time_format = "(date_trunc('year', {}) + interval '1 year - 1 day')::date"
        elif time_unit in ['月', 'month', 'months']:
            start_time_format = "date_trunc('month', {})::date"
            time_format = "(date_trunc('month', {}) + interval '1 month - 1 day')::date"
        elif time_unit in ['日', '天', '号', 'day', 'days']:
            start_time_format = "date_trunc('day', {})::date"
            time_format = "date_trunc('day', {})::date"
        else:
            raise AttributeError('Invalid {}'.format(self.time_unit_key))

        # Group clause
        group2format = dict()
        group_fields = key2field.get(self.group_key) or []
        part_partition_fields = []
        if key2field.get(self.date_key) not in [None, self.system_time]:
            raw_date_field = key2field.get(self.date_key)
            start_date_field = start_time_format.format(raw_date_field)
            date_field = time_format.format(raw_date_field)
            if raw_date_field in group_fields:
                group_fields.remove(raw_date_field)
            part_group_fields = list(group_fields)
            group_fields.append(date_field)
            part_partition_fields.append('date')
        else:
            part_group_fields = list(group_fields)
            start_date_field = start_time_format.format('current_date')
            date_field = time_format.format('current_date')
        if start_time_format != time_format:
            group2format[date_field] = '(%s, {})' % start_date_field

        # Multi-option clause
        multi_option_fields = key2field.get(self.multiple_options_key) or []
        if is_debug:
            field2options = {
                'A.migrate_type': [
                    '注销疑似去向',
                    '注册地址变更',
                    '对外投资'
                ],
                'A.type.name': [
                    '高新技术企业',
                    '重点关注'
                ]
            }
        else:
            field2options = {
                field_: [
                    dict_obj_[field_.split('.')[-1]]
                    for dict_obj_ in exec_sql(
                        'SELECT DISTINCT {} FROM {}{}'.format(
                            field_.replace('.', '_', 1)
                            if field_.count('.') > 1
                            else field_.split('.', 1)[-1],

                            key2field[self.table_key]
                            if field_.startswith('A.')
                            else key2field[self.on_table_key],

                            ' A CROSS JOIN json_to_recordset({0}::json) AS '
                            'A_{0}({1} text)'.format(
                                *field_.split('.')[1:]
                            ) if field_.count('.') > 1 else ''
                        )
                    )
                ]
                for field_ in multi_option_fields
            }
        multi_option_lengths = [
            len(field2options[field_]) for field_ in multi_option_fields
        ]
        indexes_list = [
            indexes_
            for indexes_ in itertools.product(
                *[[0, 1]] * sum(multi_option_lengths)
            )
            if any(indexes_)
        ]
        multi_where_condition2info = dict()
        for indexes in indexes_list:
            conditions, infos = [], []
            for multi_option_field, start, end in zip(
                    multi_option_fields,
                    [0, *multi_option_lengths],
                    [*multi_option_lengths, None]
            ):
                options = sorted(
                    v_ for v_, index_ in
                    zip(field2options[multi_option_field], indexes[start:end])
                    if index_
                )
                conditions.append(
                    self.gen_multi_conditions(multi_option_field, options)
                )
                infos.append(
                    '{}:{}'.format(multi_option_field, ','.join(options))
                )
            multi_where_condition2info[
                '({})'.format(' AND '.join(conditions))
            ] = '|'.join(infos)

        # Group cube clause
        group_cube_fields = [
            field_
            for field_ in (key2field.get(self.dynamic_conditions_key) or [])
            if field_ not in multi_option_fields
        ]

        # Extracting nested fields from on fields
        on_nested_fields = [
            self.nested_pat.search(field_).group()
            for field_ in key2field.get(self.on_fields_key, [])
            if self.nested_pat.search(field_)
        ]

        # Cross join JSON
        js_keys = set()
        for field in key2field[self.conditions_key]:
            js_keys.update(
                mat_.group() for mat_ in self.nested_pat.finditer(field)
            )
        for field in [
            *group_fields, *key2field.get(self.detail_fields_key, []),
            *key2field.get(self.dynamic_conditions_key, []),
            *key2field.get(self.multiple_options_key, []),
            *on_nested_fields
        ]:
            if self.nested_pat.fullmatch(field):
                js_keys.add(field)
        if js_keys:
            cross_join = ' CROSS JOIN '
            field2keys = collections.defaultdict(set)
            for js_key in js_keys:
                field, key = js_key.rsplit('.', 1)
                field2keys[field].add(key)
            left, right = '', ''
            for field, keys in field2keys.items():
                cross_info = '{}json_to_recordset({}::json) AS {}({})'.format(
                    cross_join, field, field.replace('.', '_'),
                    ', '.join('{} text'.format(key_) for key_ in keys)
                )
                if field.startswith('A.'):
                    left += cross_info
                else:
                    right += cross_info
            on_info = left + on_info + right

        if multi_where_condition2info:
            sql = '\nUNION ALL\n'.join(
                self.join_sql(
                    key2field, start_date_field, date_field, from_table,
                    on_info, js_keys, group2format, group_fields,
                    group_cube_fields, part_group_fields, time_unit,
                    part_partition_fields, info_,
                    where_info + '{}{}'.format(
                        ' AND ' if where_info else ' WHERE ',
                        multi_where_condition_
                    ),
                    is_debug
                )
                for multi_where_condition_, info_ in
                multi_where_condition2info.items()
            )
        else:
            sql = self.join_sql(
                key2field, start_date_field, date_field, from_table, on_info,
                js_keys, group2format, group_fields, group_cube_fields,
                part_group_fields, time_unit, part_partition_fields,
                '', where_info, is_debug
            )
        sql += ';'

        return sql

    def gen_multi_conditions(self, field, options):
        mat = self.nested_pat.search(field)
        condition_templates = []
        if mat:
            condition_templates.append(
                "({field} @> '[{left}{mark}{key}{mark}: "
                "{mark}{value}{mark}{right}]')".format(
                    value='%s',
                    field=mat.group(1), key=mat.group(2),
                    left='{', right='}', mark='"'
                )
            )

        if self.is_and:
            condition_templates.append("({} = '%s')".format(field))
            condition = ' AND '.join(
                itertools.chain(*[
                    [template_ % v_ for v_ in options]
                    for template_ in condition_templates
                ])
            )
        else:
            conditions = []
            if condition_templates:
                conditions.append(
                    '(' + ' OR '.join(
                        itertools.chain(*[
                            [template_ % v_ for v_ in options]
                            for template_ in condition_templates
                        ])
                    ) + ')'
                )
            conditions.append(
                '({} in ({}))'.format(
                    field, ', '.join("'{}'".format(v_) for v_ in options)
                )
            )
            condition = ' AND '.join(conditions)

        return condition

    def gen_index_name(self, key2field: dict, info, multi_colon=True):
        pat = re.compile(r'\s*[:：]\s*')
        pat_prefix = re.compile(r'^[AB]')

        field2default = dict([
            field_value_.split(':') for field_value_ in info.split('|')
            if field_value_
        ])

        part_dynamics = [
            (
                part_.strip(),
                bool(
                    pat_prefix.match(part_.strip())
                    and part_.strip() not in field2default
                )
            ) for part_ in pat.split(key2field[self.index_temp_key])
            if part_.strip()
        ]

        if multi_colon:
            index_name = " || ':' || ".join(
                "COALESCE({}::text, '{}')".format(
                    field_, self.default_null
                ) if is_dynamic_ else
                "'{}'".format(field2default.get(field_, field_))
                for field_, is_dynamic_ in part_dynamics
            )
        else:
            index_name = "COALESCE({}::text, '')".format(
                part_dynamics[0][0]
            ) if part_dynamics[0][1] else "'{}'".format(
                field2default.get(part_dynamics[0][0], part_dynamics[0][0])
            )
            index_name += ' || ' if part_dynamics[1:] else ''
            index_name += " || ".join(
                "(CASE WHEN {0} is NULL THEN '' ELSE ':' || {0} END)".format(
                    field_
                ) if is_dynamic_
                else "':{}'".format(field2default.get(field_, field_))
                for field_, is_dynamic_ in part_dynamics[1:]
            )

        return index_name

    def gen_group_values(self, key2field: dict):
        pat = re.compile(r'(?<=[A-Z]\.)(\w+)\.(\w+).*')
        pat_field = re.compile(r'([A-Z])\.')
        quote, left, right, table_short = '"', '{', '}', 'z'

        # Where clause
        cross_field2keys = collections.defaultdict(set)
        if key2field.get(self.group_conditions_key):
            conditions = []
            for condition in key2field[self.group_conditions_key]:
                mat = pat.search(condition)
                if mat:
                    conditions.append('{}_{}'.format(table_short, mat.group()))
                    cross_field2keys[mat.group(1)].add(mat.group(2))
                else:
                    conditions.append(condition.split('.', 1)[-1])
            where_info = ' WHERE ' + ' AND '.join(conditions)
        else:
            where_info = ''

        field_tables = {
            (
                pat_field.sub('', field_, 1),
                key2field[self.table_key] if
                (field_.startswith('A.') or ' A.' in field_)
                else key2field.get(self.on_table_key)
            )
            for field_ in key2field.get(self.group_key) or []
        }
        if field_tables:
            fields = []
            for field, table in field_tables:
                part_cross_field2keys = collections.defaultdict(set)
                part_cross_field2keys.update(cross_field2keys)
                if '.' in field:
                    left, key = field.rsplit('.', 1)
                    part_cross_field2keys[left].add(key)
                    new_field = '{}_{}'.format(table_short, field)
                else:
                    new_field = field
                from_table = '{} {}'.format(table, table_short)
                for cross_field, keys in part_cross_field2keys.items():
                    if not keys:
                        continue
                    from_table = '{} {}'.format(
                        from_table,
                        'CROSS JOIN json_to_recordset({0}.{1}::json) '
                        'AS {0}_{1}({2})'.format(
                            table_short, cross_field, ', '.join(
                                '{} text'.format(key_) for key_ in keys
                            )
                        )
                    )
                fields.append(' || '.join([
                    "'{quote}%s{quote}: [{quote}'",
                    "array_to_string("
                    "array(select distinct %s from %s), '{quote}, {quote}'"
                    ")",
                    "'{quote}]'"
                ]) % (
                    field.replace("'", "''"), new_field,
                    from_table + where_info
                ))
            join_field = "|| ', ' || ".join(fields).format(
                quote=quote, left=left, right=right
            )
            field_sql = "('{' || %s || '}')::json" % join_field
        else:
            field_sql = "''"

        return field_sql + ' AS group_values'

    def add_like(self, detail_sql, search_key, search_value):
        """
        Adding like claus to a SQL.
        """
        # Auditing parameters
        assert isinstance(search_key, str) and len(search_key.strip()) > 2

        search_value1 = search_value.replace('（', '(').replace('）', ')')
        search_value2 = search_value.replace('(', '（').replace(')', '）')

        # Setting parameters
        # '%' 在 sql 和 Python 中都有意义 这里先将sql 里的'%' 替换掉，格式化后再替换回来
        key, key1, key2, percent, replacer = 'w_w_w', 'm_m_m', 'n_n_n', '%', '龘'
        where_pat = re.compile(r'where', flags=re.IGNORECASE)
        separators = [r'group\s+by', r'order\s+by', 'limit']

        # Like information
        like_fields = set()
        for raw_field in self.vertical_pat.split(search_key):
            field = raw_field.strip()
            if field:
                if field.count('.') > 1:
                    new_field = field.replace('.', '_', 1)
                else:
                    new_field = field
                like_fields.add(new_field + '::text')
        like_info = ' OR '.join(
            "{} ILIKE '%({})s' OR {} ILIKE '%({})s' OR {} ILIKE '%({})s'".format(like_field_, key, like_field_, key1,
                                                                                 like_field_, key2)
            for like_field_ in like_fields
        )

        # Adding like clause
        like_sql = None
        align_sql = detail_sql.replace(percent, replacer)
        for separator in separators:
            parts = re.split(
                r'({})'.format(separator), align_sql, flags=re.IGNORECASE
            )
            assert len(parts) in {1, 3}
            if len(parts) == 3:
                and_symbol = 'AND' if where_pat.search(parts[0]) else 'WHERE'
                like_sql = '{} {} ({}) {}'.format(
                    parts[0], and_symbol, like_info, ''.join(parts[1:])
                )
                break
        if like_sql is None:
            and_symbol = 'AND' if where_pat.search(align_sql) else 'WHERE'
            like_sql = '{} {} ({})'.format(
                align_sql.rstrip(';'), and_symbol, like_info
            )

        sql = (like_sql % {key: '%{}%'.format(search_value), key1: '%{}%'.format(search_value1),
                           key2: '%{}%'.format(search_value2)}).replace(replacer, percent)
        return sql

    def add_where(self, detail_sql, where_value):
        # Auditing parameters
        assert isinstance(where_value, str) and len(where_value.strip()) > 2

        # Setting parameters
        key, percent, replacer = 'w_w_w', '%', '龘'
        where_pat = re.compile(r'where', flags=re.IGNORECASE)
        separators = [r'group\s+by', r'order\s+by', 'limit']

        # Adding where clause
        sql = None
        align_sql = detail_sql.replace(percent, replacer)
        for separator in separators:
            parts = re.split(
                r'({})'.format(separator), align_sql, flags=re.IGNORECASE
            )
            assert len(parts) in {1, 3}
            if len(parts) == 3:
                and_symbol = 'AND' if where_pat.search(parts[0]) else 'WHERE'
                sql = '{} {} ({}) {}'.format(
                    parts[0], and_symbol, where_value, ''.join(parts[1:])
                )
                break
        if sql is None:
            and_symbol = 'AND' if where_pat.search(align_sql) else 'WHERE'
            sql = '{} {} ({});'.format(
                align_sql.rstrip(';'), and_symbol, where_value
            )

        sql = sql.replace(replacer, percent)
        return sql


    def join_sql(
            self, key2field, start_date_field, date_field, from_table,
            on_info, js_keys, group2format, group_fields, group_cube_fields,
            part_group_fields, time_unit, part_partition_fields,
            info, where_info, is_debug
    ):
        area_pat = re.compile(r'[A-Z]\.')

        # Where clause of details
        if group_fields or group_cube_fields:
            group_sep = ' {} '.format(
                'AND' if where_info else 'WHERE'
            ) if group_fields else ''
            group_where = ' AND '.join(
                '{} IS NOT NULL'.format(group_field_)
                for group_field_ in group_fields
            ) if group_fields else ''
            group_conditions, js_group_conditions = [], []
            for field in group_fields:
                group_conditions.append(
                    "{} = ''' || {} || '''".format(
                        field.replace("'", "''"), field
                    )
                )
                mat = self.nested_pat.search(field)
                if mat:
                    js_group_conditions.append(
                        "{field} @> ''[{left}{mark}{key}{mark}: {mark}'"
                        " || {field_key} || '{mark}{right}]''".format(
                            field_key=field,
                            field=mat.group(1), key=mat.group(2),
                            left='{', right='}', mark='"'
                        )
                    )
            group_cube_conditions, js_group_cube_conditions = [], []
            for field in group_cube_fields:
                group_cube_conditions.append(
                    "CASE WHEN {0} is NULL THEN '' "
                    "ELSE ' AND {0} = ' || '''' || coalesce({0}, '') || '''' "
                    "END".format(field)
                )
                mat = self.nested_pat.search(field)
                if mat:
                    js_group_cube_conditions.append(
                        "CASE WHEN {field_key} is NULL THEN '' ELSE "
                        "' AND {field} @> ''[{left}{mark}{key}{mark}: {mark}'"
                        " || {field_key} || '{mark}{right}]''' END".format(
                            field_key=field,
                            field=mat.group(1), key=mat.group(2),
                            left='{', right='}', mark='"'
                        )
                    )
            inside_where_info = '{}{}'.format(
                (
                        (where_info + group_sep + group_where).replace(
                            "'", "''"
                        ) + (' AND ' if group_fields else '')
                ) if where_info or group_where else ' WHERE ',
                ' AND '.join([
                    *js_group_conditions, *group_conditions
                ])
                + (
                    "' || "
                    if (group_fields or where_info) and group_cube_fields
                    else ''
                )
                + " || ".join([
                    # TODO js_group_cube_conditions with js_group_conditions?
                    *js_group_cube_conditions,
                    *group_cube_conditions
                ]) + (" || '" if group_cube_fields else '')
            )
            if not (group_fields or group_cube_fields):
                inside_where_info = inside_where_info.replace('AND ', '', 1)
            group_by = ' GROUP BY {}{}{}'.format(
                ', '.join(
                    group2format.get(field_, '{}').format(field_)
                    for field_ in group_fields
                ),
                ', ' if group_fields and group_cube_fields else '',
                'cube({})'.format(
                    ', '.join(
                        '{}'.format(field_) for field_ in group_cube_fields
                    )
                ) if group_cube_fields else ''
            )
        else:
            group_where = ''
            inside_where_info, group_by = where_info.replace("'", "''"), ''

        # Replace JSON key
        group_cube_fields = [
            self.replace_js_key(field_, js_keys)
            for field_ in group_cube_fields
        ]
        part_group_fields = [
            self.replace_js_key(field_, js_keys)
            for field_ in part_group_fields
        ]

        # Detail SQL order by
        order_by_info = ' ORDER BY {}'.format(
            ' AND '.join(key2field[self.detail_orders_key])
        ) if key2field.get(self.detail_orders_key) else ''
        if order_by_info:
            order_by_info += ' ' + key2field[self.detail_order_type_key]

        # Fields
        is_distinct = bool(key2field.get(self.distinct_key))
        index_name = self.gen_index_name(key2field, info)
        cal_field = '{}({})'.format(
            key2field[self.formula_key],
            (
                'DISTINCT {}'.format(key2field[self.distinct_key])
                if is_distinct else '1'
            )
            if key2field[self.formula_key] == 'count' else
            key2field[self.cal_property_key]
        ) if key2field[self.formula_key] else key2field[self.cal_property_key]
        if self.where_in_value:
            cal_field += ' FILTER ({} )'.format(where_info)
            where_info = '{}{}'.format(
                ' WHERE ' if group_where else '', group_where
            )
        fields = [
            "md5({}) AS index_id".format(index_name),
            index_name + ' AS index_name',
            cal_field + ' AS value',
            start_date_field + ' AS start_date',
            date_field + ' AS date',
            '{} AS area'.format(
                key2field[self.area_key]
                if area_pat.match(key2field[self.area_key])
                else "'{}'".format(key2field[self.area_key])
            ),
            self.gen_detail(
                key2field, is_distinct,
                from_table + on_info.replace("'", "''") + inside_where_info
                + order_by_info,
                is_debug
            ),
            'now() AS create_date',
            'now() AS update_date',
            '{} AS {}'.format(
                " || '|' || ".join(
                    "'{}:' || COALESCE({}::text, '{}')".format(
                        field_.replace("'", "''"), field_, self.default_null
                    ) if field_ != info else "'{}'".format(info)
                    for field_ in sorted(
                        group_cube_fields + ([info] if info else [])
                    )
                ) if group_cube_fields or info else "''",
                self.dynamic_select_info
            ),
            '{} AS group_info'.format(
                " || '|' || ".join(
                    "'{}:' || COALESCE({}::text, '{}')".format(
                        field_.replace("'", "''"), field_, self.default_null
                    )
                    for field_ in sorted(part_group_fields)
                ) if part_group_fields else "''"
            ),
            "'{}' AS {}".format(time_unit, self.time_unit_key),
            self.gen_group_values(key2field)
        ]
        if key2field.get(self.raw_field2value_key):
            fields.extend(
                "'{}' AS {}".format(value_, name_) for name_, value_ in
                key2field[self.raw_field2value_key].items()
            )

        # Generating SQL
        # Calculating the second formula if necessary
        # 同比
        #     (2020-11 - 2019-11) / 2019-11
        # 环比
        #     (2020-11 - 2020-10) / 2020-10
        second_formula = key2field.get(self.second_formula_key)
        if second_formula in {'同比', '环比', '同比差', '环比差'}:
            partition_fields = [
                field_ for field_ in group_fields if field_ != date_field
            ]
            partition_fields.append("date_part('{}', {})".format(
                'month' if '同比' in second_formula else 'year',
                date_field
            ))
            fields.insert(
                3,
                'lead({}, 1) over({}ORDER BY {} DESC) AS lead_value'.format(
                    cal_field,
                    'PARTITION BY {} '.format(', '.join(partition_fields))
                    if partition_fields else '',
                    date_field
                )
            )
            field_names = [
                field_.rsplit('AS', 1)[-1].strip() for field_ in fields
            ]
            second_fields = [
                '{} AS value'.format(
                    '(value - lead_value)' if '差' in second_formula else
                    '(1.0 * value / lead_value - 1) * 100'
                ),
                *[
                    field_ for field_ in field_names
                    if field_ not in {'value', 'lead_value'}
                ]
            ]
            sql = 'SELECT {} FROM ({}) as t where lead_value != 0'.format(
                ', '.join(second_fields),
                'SELECT {} FROM {}'.format(
                    ', '.join(fields),
                    from_table + on_info + where_info + group_by,
                )
            )
        elif second_formula == '累加':
            field_names = [
                field_.rsplit('AS', 1)[-1].strip() for field_ in fields
            ]
            second_fields = [
                'sum(value) over (order by date) AS value',
                *[
                    field_ for field_ in field_names
                    if field_ not in {'value'}
                ]
            ]
            sql = 'SELECT {} FROM ({}) as t'.format(
                ', '.join(second_fields),
                'SELECT {} FROM {}'.format(
                    ', '.join(fields),
                    from_table + on_info + where_info + group_by,
                )
            )
        elif second_formula == '占比':
            first, second, prop = 'first_table', 'second_table', 'proportion'
            # Group fields difference partition field
            name2field = {
                field_.rsplit('.', 1)[-1]: field_
                for field_ in set(part_group_fields).difference([
                    key2field[self.proportion_key]
                ])
            }
            partition_fields = [
                self.dynamic_select_info,
                *name2field, *part_partition_fields
            ]
            field_names = [
                field_.rsplit('AS', 1)[-1].strip() for field_ in fields
            ]
            for name in set(name2field).difference(field_names):
                fields.append('{} AS {}'.format(name2field[name], name))
            second_fields = [
                'CASE WHEN {0}.value = 0 THEN 0.0 ELSE 100.0 * {0}.value / '
                'SUM({0}.value) OVER(PARTITION BY {1}) END AS value'.format(
                    first,
                    ', '.join(
                        '{}.{}'.format(first, field_)
                        for field_ in partition_fields
                    )
                ),
                *[
                    '{}.{}'.format(first, field_) for field_ in field_names
                    if field_ != 'value'
                ]
            ]
            sql = 'SELECT {} from ({}) AS {}'.format(
                ', '.join(second_fields),
                'SELECT {} FROM {}'.format(
                    ', '.join([*fields, '{} AS {}'.format(
                        key2field[self.proportion_key], prop
                    )]),
                    from_table + on_info + where_info + group_by
                ),
                first
            )
        else:
            sql = 'SELECT {} FROM {}'.format(
                ', '.join(fields),
                from_table + on_info + where_info + group_by
            )

        # Replace JSON key
        sql = self.replace_js_key(sql, js_keys)

        return sql

    def gen_detail(self, key2field, is_distinct, from_info, is_debug):
        date_key2type = {'current_date': 'date', 'now()': 'timestamp'}

        raw_info = from_info
        for date_key, date_type in date_key2type.items():
            from_info = from_info.replace(
                date_key,
                "(''' || {} || '''::{})".format(date_key, date_type)
            )
        if is_debug and raw_info != from_info:
            print(
                'Date replaced of {}:'.format(
                    key2field[self.raw_field2value_key]['rule_id']
                ),
                raw_info, '->', from_info, sep='\n\t'
            )
        return "'SELECT {}{} FROM {};' AS details_rule".format(
            'DISTINCT ' if is_distinct else '',
            ', '.join(key2field[self.detail_fields_key]),
            from_info
        )

    @staticmethod
    def replace_js_key(sql, js_keys):
        for js_key in js_keys:
            sql = sql.replace(js_key, js_key.replace('.', '_', 1))
        return sql

    @staticmethod
    def update_params(**kwargs):
        # Setting parameters
        dynamic_select_info_key = 'dynamic_select_info'
        group_info_key = 'group_info'
        is_select_key = 'is_select'
        vertical, comma = '|', ','
        pat_semi = re.compile(r'(?<!:):(?!:)')

        if kwargs[is_select_key]:
            # Parsing dynamic_select_info
            dynamic_select_info = kwargs[dynamic_select_info_key]  # type: str
            dynamic_parts = [
                part_ for part_ in
                (dynamic_select_info or '').split(vertical)
                if part_
            ]
            is_multi = False
            field2option = dict()
            for dynamic_part in dynamic_parts:
                field, options_str = pat_semi.split(dynamic_part)
                if field in field2option:
                    is_multi = True
                field2option[field] = comma.join(
                    sorted(options_str.split(comma))
                )

            # Appending group_info
            if not is_multi:
                for group_part in (kwargs[group_info_key] or '').split(
                        vertical
                ):
                    if group_part:
                        field, options_str = pat_semi.split(group_part)
                        field2option[field] = options_str

            # Updating dynamic_select_info
            kwargs[dynamic_select_info_key] = vertical.join(
                '{}:{}'.format(field_, option_) for field_, option_ in
                sorted(
                    field2option.items(),
                    key=lambda x: dynamic_select_info.find(x[0])
                )
            )
        # Updating group_info
        kwargs[group_info_key] = None

        return kwargs

    def gen_front_fields(self):
        # Setting variables
        first_formulas = ['count', 'sum', 'avg', 'max', 'min']
        second_formulas = [None, '同比', '占比']

        js = {
            'type2name2field': {
                'single_box': {
                    '新增还是重用指标': 'is_add',
                    '指标名模板': self.index_temp_key,
                    '是否手写SQL': 'is_handwritten',
                    '主表名': self.table_key,
                    '关联表': self.on_table_key,
                    '关联字段': self.on_fields_key,
                    '计算公式': self.formula_key,
                    '算占比的分母字段': self.proportion_key,
                    '变量名称': self.cal_property_key,
                    '限制条件': self.conditions_key,
                    '分组字段': self.group_key,
                    '时间维度字段': self.date_key,
                    '所属区域': self.area_key,
                    '需要去重统计的字段': self.distinct_key,
                    '是否有效': 'is_valid',
                    '模块名': 'product_name',
                    '明细字段': self.detail_fields_key,
                    '精度': 'float',
                    '是否直接取详情': 'is_detail',
                },
                'group_box': {
                    '单位': 'unit',
                    '指标类型': 'index_type',
                    '指标说明': 'explain',
                    '更新周期': 'frequency',
                    '数据来源': 'date_source',
                    '上层级编码': 'up_code',
                    '上层级编码名称': 'up_name',
                },
                'check_box': {
                    '详情SQL': 'details_rule',
                    '指标SQL': 'rule',
                }
            },
            'field2options': {
                'is_add': ['新增', '重用'],
                'is_handwritten': ['是', '否'],
                'formular': [
                    '{}{}'.format(
                        first_,
                        '' if second_ is None else '|{}'.format(second_)
                    )
                    for first_, second_ in
                    itertools.product(first_formulas, second_formulas)
                ],
                'is_valid': [0, 1],
            }
        }

        return js


if __name__ == '__main__':
    self = SQLGenerator()
    field2value = {
        'main_chart': 'ods_excellent_company_business_index',
        'index_name': '高精尖纳税总额：A.field：同比',
        'formular': 'sum|同比',
        'variable': 'A.tax/10000',
        'restrict': '',
        'remove_repeat': None,
        'denominator': '',
        'polymeric': 'A.field|A.date',
        'date_field': 'A.date',
        'unit': '%',
        'product_code': 'SFZ0004',
        'product_name': '重点企业净资产、利润、纳税',
        'related_chart': '',
        'related_field': '',
        'details_field':
            'A.company_name|A.field|A.company_type|A.address_area',
        'index_type': '当年值',
        'explain': None,
        'frequency': '',
        'date_source': None,
        'area': '',
        'up_code': '高精尖企业财务数据统计',
        'up_nUme': 'U0003',
        'rule_id': 'R00000035',
        'is_valid': ''
    }
    self.conf_to_sql(field2value)
