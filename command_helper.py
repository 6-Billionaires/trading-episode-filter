from optparse import OptionParser
import sys

from optparse import OptionParser
import sys

if(len(sys.argv) <= 1): # 인자 첫번째는 실행 파일이 들어가므로 무조건 길이는 1 이상이다.
	print("preprocessor.py -h 또는 --help 를 입력해 메뉴얼을 참조하세요.")
	exit()

use = "Usage: %prog [options]"
parser = OptionParser(usage = use)
parser.add_option("-s", "--startdate", dest="start_date", metavar="YYYYMMDD", default=False, help="Start date")
parser.add_option("-e", "--enddate", dest="end_date", metavar="YYYYMMDD", default=False, help="End date")
parser.add_option("-r", "--minimun", dest="min_rate", default=False, help="Minimun rate")
parser.add_option("-g", "--maximun", dest="max_rate", default=False, help="Maximun rate")
parser.add_option("-t", "--type", dest="episode_type", metavar="episode_type", default=False, help="Episode type name")

options, args = parser.parse_args()

if options.start_date:
	start_date = options.start_date
else:
	print("[-s] option is mandatory..")
	exit()

if options.end_date:
	end_date = options.end_date
else:
	print("[-e] option is mandatory..")
	exit()

if options.min_rate:
	min_rate = options.min_rate
else:
	print("[-r] option is mandatory..")
	exit()

if options.max_rate:
	max_rate = options.max_rate
else:
	print("[-g] option is mandatory..")
	exit()

if options.episode_type:
	episode_type = options.episode_type
else:
	print("[-t] option is mandatory..")
	exit()