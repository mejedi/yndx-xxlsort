#! /usr/bin/python

import sys, re, hashlib, random, argparse


def generate(size_spec, rng):
    m = re.match(r'(\d+)([kKmMgG]?)', size_spec)
    if not m:
        raise Exception('Bad size spec: '+size_spec)
    size = int(m.group(1)) * {'':1, 'k':1024, 'm':1024*1024, 'g':1024*1024*1024}[m.group(2).lower()]
    generated = 0
    i = 0
    sha256 = hashlib.sha256
    randint = random.randint;
    lognorm = random.lognormvariate;
    while generated < size:
        key = sha256(str(i)).hexdigest()
        flags = randint(0, 0xffffffffffffffff)
        crc = randint(0, 0xffffffffffffffff)
        seed = randint(0, 0xffffffffffffffff)
        body_size = int(rng()) 

        print key, flags, crc, body_size, seed
        generated += 88 + body_size
        i += 1

#
# small 3   2.3
# large 5.2 3.2
#

if __name__ == '__main__':
    lognorm = random.lognormvariate;
    parser = argparse.ArgumentParser(description='Generate sample sata for xxlsort')
    parser.add_argument('size_spec', metavar='SIZE', type=str, default='20G',
                        help='henerate this many data')
    parser.add_argument('--large', dest='rng', action='store_const',
                        const=lambda:lognorm(5.2,3.2),
                        default=lambda:lognorm(3.0,2.3),
                        help='generate larger bodies')
    args = parser.parse_args()
    generate(args.size_spec, args.rng)

