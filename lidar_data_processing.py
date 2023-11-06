import whitebox_workflows as wbw

wbe = wbw.WbEnvironment()

wbe.working_directory = wbw.download_sample_data('Kitchener_lidar')
print(f'Data have been stored in: {wbe.working_directory}')

lidar = wbe.read_lidar('Kitchener_lidar.laz')
num_points = lidar.header.get_num_points()

lidar_out = wbe.new_lidar(lidar.header)
lidar_out.vlr_data = lidar.vlr_data

print('Filtering point data...')
old_progress = -1
for i in range(num_points):
    point_data, time, colour, waveform = lidar.get_point_record(i)
    
    if point_data.is_first_return() or point_data.is_intermediate_return():
        lidar_out.add_point(point_data, time, colour, waveform)
    
    progress = int((i + 1.0) / num_points * 100.0)
    if progress != old_progress:
        old_progress = progress
        print(f'Progress: {progress}%')

wbe.write_lidar(lidar_out, 'new_lidar.laz')
print('Done!')


