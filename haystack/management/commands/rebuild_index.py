from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from haystack.management.commands.clear_index import Command as ClearCommand
from haystack.management.commands.update_index import Command as UpdateCommand
from optparse import make_option
import urlparse
import pysolr
import re
import random


class Command(BaseCommand):
    help = "Completely rebuilds the search index by removing the old data and then updating."
    base_options = (
        make_option('--use-temp', action='store_true', dest="use_temp",
            help='Build Index on temp core then swap',
        ),
    )

    option_list = (base_options +
                   BaseCommand.option_list +
                   ClearCommand.base_options +
                   tuple([option for option in UpdateCommand.base_options if option.get_opt_string() != '--site']))
    
    def handle(self, **options):
        if options['use_temp'] and settings.HAYSTACK_SEARCH_ENGINE == "solr":
            self._index_update_with_temp(options)
        else:
            call_command('clear_index', **options)
            call_command('update_index', **options)

    def _get_core_instance_dir(self, sca, core):
        res = sca.status(core)
        match = re.search(r'<str name="instanceDir">(?P<path>[^<]+)</str>', res)
        if match:
            return match.groupdict()['path']
        else:
            raise ValueError("No instance dir found for %s" % core)

    def _index_update_with_temp(self, options):
        """
        Create a new index, update index there then replace existing
        with new index.

        Caveats:
        Solr only for now.
        Requires solr.xml to have persistent=true or bad things happen
        """
        bits = urlparse.urlparse(settings.HAYSTACK_SOLR_URL)
        sca = pysolr.SolrCoreAdmin('%s://%s' % bits[:2])
        core = bits.path.split('/')[-1]
        instance_dir = self._get_core_instance_dir(sca, core)
        hash = hex(random.getrandbits(128))[2:10]
        new_core_name = '%s_%s' % (core, hash)

        #this data dir is specific to our implementation...
        sca.create(new_core_name, instance_dir=instance_dir,
            data_dir='/var/lib/solr/%s/data' % new_core_name)

        options['path'] = '%s://%s/solr/%s' % (
            bits.scheme,
            bits.netloc,
            new_core_name
        )
        call_command('update_index', **options)
        sca.swap(core, new_core_name)
        sca.unload(new_core_name, delete_data_dir=True, delete_instance_dir=True, delete_index=True)
