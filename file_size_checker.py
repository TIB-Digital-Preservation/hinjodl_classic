# -*- coding: utf-8 -*-

# Copyright and licensing information given in the main script
# 'hindawi-downloader.py' apply.

# The Tiny File Size Checker looks for files that raise suspicion due
# to their size.

# 'parent_folder' is considered user input.

# Cases:
#   * Files are 0 Bytes in size. This is bad and usually needs fixing.
#   * Files are suspiciously small. Those could still be okay.
#   * Files are big. No one cares. This just happens.
#   * Files are really big. Maybe you want to know why.

# Thresholds:
# | Empty File |  Small File  | Big File  | Large File |
# |   0 Bytes  | < 1024 Bytes | > 100 MiB |  > 1 GiB   |

# This helper script is specific to the folder structure produced by the
# Hindawi Journal Downloader as only the contents of MASTER folders are
# taken into account, while ignoring MD5 side car files.


import os

# parent folder containing the files you want to check
parent_folder = 'download_destination/TEST'


def check_file_sizes(current_folder):

    """Raises alarm when file sizes are suspicious."""

    # print(f'Checking file sizes in {current_folder}.')

    supplementary_materials_exist = False
    files = []
    with os.scandir(current_folder) as contents:
        for art_item in contents:
            if art_item.is_file():
                files.append(art_item)
            elif art_item.name == 'supplements':
                supplementary_materials_exist = True

    if supplementary_materials_exist:
        with os.scandir(os.path.join(current_folder, 'supplements')) as sup_contents:
            for sup_item in sup_contents:
                if sup_item.is_file():
                    files.append(sup_item)
                else:
                    print(f'WARNING: A supplement in {current_folder} is not a file. Please investigate.')

    for file in files:
        bytesize = file.stat().st_size
        mibsize = bytesize/(1024**2)
        if bytesize == 0:
            detected_files['Empty'].append(file.path)
        elif bytesize < 1024 and '.md5' not in file.name:
            detected_files['Small'].append(file.path)
        elif mibsize > 1024:
            detected_files['Large'].append(file.path)
        elif mibsize > 100:
            detected_files['Big'].append(file.path)


# results populate this dictionary
detected_files = {'Empty': [], 'Small': [], 'Big': [], 'Large': []}


print(f'Checking file sizes in {parent_folder}.')

# get all the MASTER folders
content_folders = []
for (path, folders, filenames) in os.walk(parent_folder):
    if 'MASTER' in folders:
        mpath = os.path.join(path, 'MASTER')
        content_folders.append(mpath)

print(f'Found {str(len(content_folders))} folders to check.')

for folder in content_folders:
    check_file_sizes(folder)

report_files = False
for case in detected_files:
    if detected_files[case]:
        print(f'WARNING: {case} files detected.')
        report_files = True

if not report_files:
    print('Nothing detected. All checked file sizes seem reasonable.')

if report_files is True:
    print('Writing findings in file_size_report.txt.')
    with open('file_size_report.txt', 'w') as output_file:
        for case in detected_files:
            if detected_files[case]:
                output_file.write(f'{case} files:\n')
                for entry in detected_files[case]:
                    output_file.write(f'{entry}\n')
                output_file.write('\n')

print('Done.')
