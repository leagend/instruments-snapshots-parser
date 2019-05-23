# -*- coding: utf-8 -*-


import numpy as np
import os
import sys

AVER = 'average'


def get_KiB(data_size):
    bts = 'Bytes'
    kb = 'KiB'
    mb = 'MiB'
    if data_size.endswith(bts):
        data_size = round(float(data_size.replace(bts, '')) / 1024, 2)
    elif data_size.endswith(kb):
        data_size = round(float(data_size.replace(kb, '')), 2)
    else:
        data_size = round(float(data_size.replace(mb, '')) * 1024, 2)
    return data_size

def transfer_num_2_digit(num):
    if num < 10:
        _num = '0{0}'.format(num)
    else:
        _num = '{0}'.format(num)
    return _num

class DataOrganizer(object):
    def __init__(self):
        self.result_data = {}
        self.start_time = ''
        self.end_time = ''
        pass

    def read_physical_data(self, physical_data_file):
        with open(physical_data_file, 'r') as _file:
            lines = _file.readlines()
            lines = lines[1:]
            for line in lines:
                if line != "\n":
                    data_group = line.rstrip('\n').split(',')
                    time_group = data_group[0].split('.')
                    if self.start_time == '':
                        self.start_time = time_group[0]
                    self.end_time = time_group[0]
                    # print(data_group[1:])
                    cpu = round(float(data_group[1].rstrip('%')), 2)
                    mem = data_group[2]
                    if mem.endswith('KiB'):
                        mem = round(float(mem.rstrip('KiB')) / 1024, 2)
                    else:
                        mem = round(float(mem.rstrip('MiB')), 2)
                    meta_data = {time_group[0]: {int(time_group[1]): [cpu, mem]}}
                    if time_group[0] not in self.result_data:
                        self.result_data.update(meta_data)
                    else:
                        self.result_data[time_group[0]].update(meta_data[time_group[0]])
        pass

    def fix_data(self):
        time_group = self.end_time.split(":")
        if len(time_group) == 3:
            for hour in range(0, 24):
                for min in range(0, 60):
                    for sec in range(0, 60):
                        time_str = "{0}:{1}:{2}".format(hour, min, sec)
                        if time_str not in self.result_data:
                            if time_str == "00:00":
                                continue
                            if sec == 0:
                                if min == 0:
                                    time_str_1 = "{0}:{1}:{2}".format(hour - 1, 59, 59)
                                else:
                                    time_str_1 = "{0}:{1}:{2}".format(hour, min - 1, 59)
                                self.result_data.update({time_str: self.result_data[time_str_1]})
                            else:
                                time_str_1 = "{0}:{1}:{2}".format(hour, min, sec - 1)
                            self.fill_zero_time_slot(time_str, time_str_1)
                        else:
                            self.caculate_average_physical_data_for_time_slot(time_str)
                        if time_str == self.end_time:
                            return
        else:
            for min in range(0, 60):
                for sec in range(0, 60):
                    time_str = "{0}:{1}".format(transfer_num_2_digit(min), transfer_num_2_digit(sec))
                    if time_str not in self.result_data:
                        if time_str == "00:00":
                            continue
                        if sec == 0:
                            time_str_1 = "{0}:{1}".format(transfer_num_2_digit(min - 1), 59)
                            self.result_data.update({time_str: self.result_data[time_str_1]})
                        else:
                            transfer_num_2_digit(sec)
                            time_str_1 = "{0}:{1}".format(transfer_num_2_digit(min),
                                                          transfer_num_2_digit(sec - 1))
                        self.fill_zero_time_slot(time_str, time_str_1)
                    else:
                        self.caculate_average_physical_data_for_time_slot(time_str)
                    if time_str == self.end_time:
                        return

    def fill_zero_time_slot(self, time_str, time_str_1):
        meta_data = {
            AVER: {
                'cpu': 0,
                'memory': 0,
                'upflow': 0,
                'downflow': 0
            }
        }
        if time_str_1 in self.result_data:
            meta_data[AVER]['cpu'] = self.result_data[time_str_1][AVER]['cpu']
            meta_data[AVER]['memory'] = self.result_data[time_str_1][AVER]['memory']
            if len(self.result_data[time_str_1]) < 2:
                meta_data[AVER]['cpu'] /= 2
                meta_data[AVER]['memory'] /= 2
        print("fill time slot {0}".format(time_str), meta_data)
        self.result_data.update({time_str: meta_data})

    def caculate_average_physical_data_for_time_slot(self, time_str):
        values = self.result_data[time_str].values()
        if len(values) > 1:
            array = np.array(values)
            # print(array)
            phy_means = array.mean(1).tolist()
        else:
            phy_means =values[0]
        meta_data = {
            AVER: {
                'cpu': round(phy_means[0], 2),
                'memory': round(phy_means[1], 2),
                'upflow': 0,
                'downflow': 0
            }
        }
        self.result_data[time_str].update(meta_data)

    def read_throughput_data(self, throughput_data_file):
        if os.path.isfile(throughput_data_file):
            with open(throughput_data_file, 'r') as _file:
                lines = _file.readlines()
                lines = lines[1:]
                for line in lines:
                    if line != "\n":
                        data_group = line.rstrip('\n').split(',')
                        time_group = data_group[0].split('.')
                        self.result_data[time_group[0]][AVER]['upflow'] += get_KiB(data_group[2])
                        self.result_data[time_group[0]][AVER]['downflow'] += get_KiB(data_group[1])
        pass

    def output_data_to_file(self, data_prefix):
        time_keys = self.result_data.keys()
        time_keys.sort()
        with open('{0}_result.csv'.format(data_prefix), 'w+') as _file:
            tmp_str = ''
            for time_key in time_keys:
                values = self.result_data[time_key][AVER]
                tmp_str += '{0},{1},{2},{3},{4}\n'.format(time_key, values['cpu'], values['memory'], values['upflow'], values['downflow'])
                # print(values['cpu'], values['memory'], values['upflow'], values['downflow'])
            _file.writelines(tmp_str.rstrip('\n'))


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        file_prefix = sys.argv[1]
        organizer_0 = DataOrganizer()
        get_KiB('1000Bytes')
        organizer_0.read_physical_data("{0}.csv".format(file_prefix))
        # print(organizer_0.result_data)
        organizer_0.fix_data()
        # print(organizer_0.result_data)
        organizer_0.read_throughput_data("{0}-net.csv".format(file_prefix))
        # print(organizer_0.result_data)
        organizer_0.output_data_to_file(file_prefix)
    else:
        print('Please provide the prefix of data files which are ready for organizing.')
        sys.exit(1)
