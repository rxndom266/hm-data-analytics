def driver(config):
    client_code = config['client_code']
    load_type = config['raw_input_format']
    load_id = config['load_id']
    enrich_flag = config['enrich_flag']
    batch_datetime = config['batch_datetime']
    mount_root = config['mount_root']
    client_data_path = config['client_data_path']
    file_name = config['file_name']
    load_id = config['load_id']
    client_file_path = client_data_path+'/'+file_name
    transpath = '/delta/{}/{}/transform'.format(client_code, load_type)
    cleanpath = '/delta/{}/{}/clean'.format(client_code, load_type)
    rejectpath = '/delta/{}/{}/reject'.format(client_code, load_type)
    resultpath = '/delta/{}/{}/result'.format(client_code, load_type)

    if(load_type=='activity'):
        import TransformActivity
        TransformActivity.transform(load_id, client_file_path, transpath, rejectpath)
    
    import Cleanse
    Cleanse.cleanse(load_type, transpath, cleanpath, rejectpath)
    import trial
    return transpath, cleanpath, rejectpath