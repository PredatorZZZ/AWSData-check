from datetime import datetime

from py.xml import html


class HTMLReport:

    TITLE = "AWS Data check"

    def __init__(self, file_name: str):
        self.file_name = file_name
        self.data_results = []
        self.tables_processed = 0

    def _generate_report(self):
        css_href = "assets/style.css"
        js_href = "https://cdn.dhtmlx.com/suite/edge/suite.css"
        html_css = html.link(href=css_href, rel="stylesheet", type="text/css")
        js_style = html.link(href=js_href, rel="stylesheet", type="text/css")
        head = html.head(
            html.meta(charset="utf-8"),
            html.title(self.TITLE),
            html.script(src='assets/index.js'),
            html.script(src='https://cdn.dhtmlx.com/suite/edge/suite.js'),
            html_css,
            js_style
        )
        body = html.body(
            html.h1(self.TITLE),
            html.p(f'Report generated at '
                   f'{datetime.today().strftime("%Y-%m-%d %H:%M:%S")}'),
            html.p(f'Tables processed: {self.tables_processed}'),
            html.h2("Results")
        )
        body.extend(self.data_results)
        doc = html.html(head, body)
        unicode_doc = f"<!DOCTYPE html>\n{doc.unicode(indent=2)}"
        unicode_doc = unicode_doc.encode("utf-8", errors="xmlcharrefreplace")
        return unicode_doc.decode("utf-8")

    @staticmethod
    def _generate_athena_report(athena_data: dict):
        ahtena_report = html.tr(
                [
                    html.th(
                        f'Athena results:',
                        class_='athena-results'
                    ),
                    html.th(athena_data)
                ]
            )
        return ahtena_report

    @staticmethod
    def _generate_s3_report(s3_data: dict):
        s3_link = s3_data['S3Link']
        s3_data.pop('S3Link')
        s3_report = html.tr(
            [
                html.th(
                    f'Athena results:',
                    class_='s3-results'
                ),
                html.th(s3_data,
                        html.p(
                            html.a(
                                s3_link,
                                href=s3_link
                            )
                        ),
                        )
            ]
        )
        return s3_report

    @staticmethod
    def _generate_glue_report(glue_data: dict):
        glue_report = html.tr(
            [
                html.th(
                    f'Athena results:',
                    class_='glue-results'
                ),
                html.th(glue_data),
            ]
        )
        return glue_report

    @staticmethod
    def _get_csv_data(csv_file_path: str):
        if csv_file_path and csv_file_path.endswith('.csv'):
            with open(csv_file_path, 'r') as csv_file:
                return csv_file.read()
        return "null"

    def insert_data(self, tables_data: dict):
        if not isinstance(tables_data, dict):
            raise TypeError(
                f'{self.__module__} supports only dict as InputParam'
            )

        self.tables_processed = len(tables_data.keys())

        for raw_table_name, raw_table_data in tables_data.items():
            csv_file_path = raw_table_data['Athena']['FilePath']

            database_name, table_name = raw_table_name.split('.')
            self.data_results.append(
                html.div(
                    html.h3(f"Input data: {raw_table_name}"),
                    html.table(
                        html.tr(
                            [
                                html.th('Database:',
                                        class_='database-name'),
                                html.th(database_name)
                            ]
                        ),
                        html.tr(
                            [
                                html.th('Table name:',
                                        class_='table-name'),
                                html.th(table_name)
                            ]
                        ),
                        self._generate_glue_report(raw_table_data['Glue']),
                        self._generate_s3_report(raw_table_data['S3']),
                        self._generate_athena_report(raw_table_data['Athena']),
                        class_="processed-table"
                    ),
                    html.div(f"{self._get_csv_data(csv_file_path)}",
                             table_name=table_name,
                             class_="data-tables")
                )
            )

    def save_report(self):
        with open(self.file_name, 'w') as file:
            file.write(self._generate_report())
