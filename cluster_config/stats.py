import operator
import datetime
from cluster_config import init_spark, timer

sc = init_spark.get_sc('stats')
year = 2016
qtr = 1
docs = 1000
input = 'gs://output-bitter-voice/10k_data-{year}-qtr{qtr}-{docs}'.format(**locals())

names = '1 1A 1B 2 3 4 5 6 7 7A 8 9 9A 9B 10 11 12 13 14 15 S'.split(' ')

def count(filing):
    if filing['error']:
        return dict(total=1, valid=0, size=0, time=filing['download_time'])
    else:
        total_size = 0
        for name in names:
            if name in filing:
                total_size += len(filing[name])
        return dict(total=1, valid=1, size=total_size, time=filing['download_time'])

with timer.timer(print_=True):
    risk_data = sc.pickleFile(input)
    risk_data = risk_data.map(count).collect()

    result = dict(total=0, valid=0, size=0, time=0)
    for filing in risk_data:
        for key in 'total valid size time'.split(' '):
            result[key] += filing[key]

avg_size = (result['size'] / result['valid']) / 1e3
size = result['size'] / 1e6
hit_ratio = (result['valid'] / result['total']) * 1e2
throughput = result['size'] / result['time'] / 1e3

print('''
{result[time]:.1f}s in total
{throughput:f} kbytes / sec
{size:.1f} MB in total
{avg_size:.0f} KB per doc
{hit_ratio:.0f}% = {result[valid]} / {result[total]}
'''.format(**locals()))
