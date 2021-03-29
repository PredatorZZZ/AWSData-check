from datetime import datetime

from py.xml import html


class HTMLReport:

    TITLE = "AWS Data check"

    def __init__(self, file_name):
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

    def insert_data(self, tables_data: dict):
        self.tables_processed = len(tables_data.keys())
        for raw_table_name, raw_table_data in tables_data.items():
            database_name, table_name = raw_table_name.split('.')
            file_name = f'athena_{database_name}_{table_name}.csv'
            self.data_results.append(
                html.div(
                    html.table(
                        html.h3(f"Input data: {raw_table_name}"),
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
                        *[
                            html.tr(
                                [
                                    html.th(
                                        f'{service_name} results:',
                                        class_=f'{service_name.lower()}-results'
                                    ),
                                    html.th(raw_table_data[service_name])
                                ]
                            ) for service_name in raw_table_data.keys()
                        ],
                        class_="processed-table",
                        id="results-table",
                    ),
                    html.div(id=raw_table_name,
                             filename=file_name,
                             class_="data_tables")
                )
            )

    def save_report(self):
        with open(self.file_name, 'w') as file:
            file.write(self._generate_report())
