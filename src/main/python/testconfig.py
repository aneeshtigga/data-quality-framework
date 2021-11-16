from configparser import ConfigParser

parser = ConfigParser()
parser.read('../../../configuration/eng/tables.ini')

print(parser.sections())
print(parser.get('review', 'src_type'))
print(parser.options('review'))
print(parser.getint('review','patitions', fallback=10))
print(parser.get('review', 'dest_type'))