from splunklib.searchcommands import dispatch, EventingCommand, Configuration, Option
import splunk.clilib.cli_common

import sys
import csv

@Configuration()
class CSVPathCommand(EventingCommand):
    conf_file = 'csvpath'
    conf_fieldsname = 'fields'
    conf_stanzas_all = splunk.clilib.cli_common.getConfStanzas(conf_file)

    conf_stanzas = set()
    conf_fields = set()
    for conf_stanza_key in conf_stanzas_all:
        conf_stanzas.add(conf_stanza_key)
        if conf_fieldsname in conf_stanzas_all[conf_stanza_key]:
            fields_string = conf_stanzas_all[conf_stanza_key][conf_fieldsname]
            for field in fields_string.split(','):
                conf_fields.add(field)

    # XXX hardcoded field name
    csv_field_name = '_raw'

    def transform(self, records):
        for record in records:
            # the first yielded result determines the fields that Splunk sees, so make sure all configured CSV Fields are returned here
            for field in self.conf_fields:
                if not field in record:
                    record[field] = None

            if self.csv_field_name in record:
                for row in csv.reader([record[self.csv_field_name]]):
                    if row[0] in self.conf_stanzas_all:
                        settings = self.conf_stanzas_all[row[0]]
                        if self.conf_fieldsname in settings:
                            fieldnames = settings[self.conf_fieldsname]
                            for row in csv.DictReader([record[self.csv_field_name]], fieldnames=fieldnames.split(',')):
                                record.update(row)
            yield record

dispatch(CSVPathCommand, sys.argv, sys.stdin, sys.stdout, __name__)
