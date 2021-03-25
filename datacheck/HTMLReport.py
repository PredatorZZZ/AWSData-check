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
        html_css = html.link(href=css_href, rel="stylesheet", type="text/css")
        head = html.head(
            html.meta(charset="utf-8"),
            html.title(self.TITLE),
            html_css
        )
        table = [
            html.h2("Results"),
            html.table(
                self.data_results,
                id="results-table",
            ),
        ]
        body = html.body(
            html.h1(self.TITLE),
            html.p(f'Report generated at '
                   f'{datetime.today().strftime("%Y-%m-%d %H:%M:%S")}'),
            html.p(f'Tables processed: {self.tables_processed}')
        )
        body.extend(table)
        doc = html.html(head, body)
        unicode_doc = f"<!DOCTYPE html>\n{doc.unicode(indent=2)}"
        unicode_doc = unicode_doc.encode("utf-8", errors="xmlcharrefreplace")
        return unicode_doc.decode("utf-8")

    def insert_data(self, tables_data: dict):
        self.tables_processed = len(tables_data.keys())
        for raw_table_name, raw_table_data in tables_data.items():
            database_name, table_name = raw_table_name.split('.')
            self.data_results.append(
                html.div(
                    html.tr(
                        [
                            html.th('Database:', bgcolor="#C0C0C0"),
                            html.th(database_name),
                            html.th('Table name:', bgcolor="#C0C0C0"),
                            html.th(table_name)
                        ]
                    ),
                    *[
                        html.div(
                            html.tr(
                                [
                                    html.th(
                                        f'{service_name} results:',
                                        colspan="4",
                                        style="text-align:center",
                                        bgcolor="#C0C0C0"
                                    )
                                ]
                            ),
                            html.tr(
                                [
                                    html.th(raw_table_data[service_name],
                                            colspan="4")
                                ]
                            ),
                            _class=f"service-{service_name.lower()}"
                        ) for service_name in raw_table_data.keys()
                    ],
                    _class="processed-table"
                )
            )

    def save_report(self):
        with open(self.file_name, 'w') as file:
            file.write(self._generate_report())
