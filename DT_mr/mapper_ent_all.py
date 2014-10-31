#! /usr/bin/env python
# data preparation from hadoop
import sys

'''
Sunny,Hot,High,False,No
Sunny,Hot,High,True,No
'''

if __name__ == "__main__":
    node = float(sys.argv[1])
    target = sys.argv[2] 
    cnt = 0

    if node > 0:
        for line in sys.stdin:
            if target in line:
                line = line.strip()
                words = line.split(',')
                category = words[-1]

                for i in range(0, len(words)):

                    key = 'var0' + str(i) + ';' + words[i]
                
                    if words[i] == category:
                        key = 'outcome;' + words[i]
                        print ('%s\t%s') % (key, 1)
                    else:
                        print ('%s\t%s') % (key, 1)
                cnt += 1
            else:
                pass

        print 'allcnt\t%s' % (cnt)
    else:
        for line in sys.stdin:
            line = line.strip()
            words = line.split(',')
            category = words[-1]

            for i in range(0, len(words)):

                key = 'var0' + str(i) + ';' + words[i]

                if words[i] == category:
                    key = 'outcome;' + words[i]
                    print ('%s\t%s') % (key, 1)
                else:
                    print ('%s\t%s') % (key, 1)
            cnt += 1

        print 'allcnt\t%s' % (cnt)
