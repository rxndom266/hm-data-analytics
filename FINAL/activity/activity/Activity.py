def driver(config):
    client_code = config['client_code']
    data_source = config['data_source']
    load_type = config['load_type']
    load_id = config['load_id']
    enrich_flag =  config['enrich_flag']=='Yes')
    batch_datetime = config['batch_datetime']
    mount_root = config['mount_root']
    client_data_path = config['client_data_path']
    file_name = config['file_name']
    load_id = config['load_id']
    client_file_path = client_data_path+'/'+file_name
    transpath = '/delta/{}/{}/{}/transform'.format(client_code, data_source, load_type)
    cleanpath = '/delta/{}/{}/{}/clean'.format(client_code, data_source, load_type)
    rejectpath = '/delta/{}/{}/{}/reject'.format(client_code, data_source, load_type)
    idpath = '/delta/{}/{}/{}/id'.format(client_code, data_source, load_type)
    enrichpath = '/delta/{}/activity/enriched'.format(client_code)
    cdmpath = '/delta/{}/activity/cdm'.format(client_code)
    personcdmpath = '/delta/{}/person/cdm'.format(client_code)
    
    from transform import Transform
    Transform.transform(load_id, load_type, data_source, client_file_path, transpath)
    
    from cleanse import Cleanse
    Cleanse.cleanse(load_id, load_type, transpath, cleanpath, rejectpath)
    
    from activity.identify import identify
    identify(load_id, cleanpath, idpath, cdmpath, personcdmpath)

    if enrich_flag==True:
        from activity.enrich import enrich
        enrich(cdmpath, enrichpath)

    return {"transform":transpath, "cleanse":cleanpath, "identify":idpath, "reject":rejectpath, "cdm":cdmpath, "enrich":enrichpath}

