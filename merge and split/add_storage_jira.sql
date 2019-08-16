CREATE OR REPLACE FUNCTION public.add_storage(storagejson json, "excluded_name" text default '' )
 RETURNS json
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_capture_id int;
    v_entity_id int;
    v_organization_id text;
    v_identifier text;
    v_storage_type storage_type;
    v_capacity_gb text;
    v_capacity_type capacity_type_enum;
    v_used_gb text;
    v_LUN text;
    v_pool text;
    v_RAID raid_level_enum;
    v_label text;
    v_description text;
    v_media_type media_type_enum;
    v_used_for text;
    v_mean_throughput numeric;
    v_peak_throughput numeric;
    v_mean_iops numeric;
    v_peak_iops numeric;
    v_access_frequency text;
    v_file_system text;
    v_mean_disk_latency numeric;
    v_peak_disk_latency numeric;
    v_dns_name text;

BEGIN
    select
        organization_id, identifier, storage_type, capacity_gb, used_gb, "LUN", pool, raid_level, label, description, mean_disk_latency, peak_disk_latency, media_type, used_for, file_system, mean_throughput, peak_throughput, mean_iops, peak_iops, access_frequency, dns_name, capacity_type
    from json_to_record(storageJSON)
    INTO
        v_organization_id,v_identifier, v_storage_type, v_capacity_gb, v_used_gb, v_LUN, v_pool, v_RAID, v_label, v_description, v_mean_disk_latency, v_peak_disk_latency, v_media_type, v_used_for, v_file_system, v_mean_throughput, v_peak_throughput, v_mean_iops, v_peak_iops, v_access_frequency, v_dns_name, v_capacity_type
    AS
        x
    (organization_id text, identifier text, storage_type storage_type, capacity_gb text, used_gb text,  "LUN" text, pool text, raid_level raid_level_enum, label text, description text, mean_disk_latency numeric, peak_disk_latency numeric, media_type media_type_enum, used_for text, file_system text, mean_throughput numeric, peak_throughput numeric,  mean_iops numeric, peak_iops numeric, access_frequency text, dns_name text, capacity_type capacity_type_enum);

    --check for null in mandatory fields
    IF (v_identifier IS NULL) OR (v_capacity_gb IS NULL) THEN
    return json_build_object(
                        'status','Failed',
                        'description','Mandatory fields not filled.'
                );
end if;

    IF (v_organization_id IS NULL) THEN
return json_build_object(
                        'status','Failed',
                        'description','Organization id missing."}'
                );
end if;

    --check cor dublicacy
    if(select entity_id
from v_entity_storages
where organization_id=v_organization_id and identifier=v_identifier and is_duplicate_of is NULL and deleted='f' and identifier <> excluded_name) is NOT NULL THEN
return json_build_object(
                        'status','Failed',
                        'description','Identifier(s) already present in warehouse.'
                );
end if;

    --make sure capacity_gb and used_gb fields are populated with numeric values
    IF (NOT (SELECT v_capacity_gb ~ '^\d+(.\d+)?$')) THEN
return json_build_object(
                        'status','Failed',
                        'description','Capacity must numeric. '
                );
END IF;

    IF (NOT (SELECT v_used_gb ~ '^\d+(.\d+)?$')) THEN
return json_build_object(
                        'status','Failed',
                        'description','Used must be numeric. '
                );
END IF;

    --insert into captures TABLE
    insert into captures
    ( organization_id,
    capture_time,
    capture_source,
    capture_comments)
values
    (
        v_organization_id,
        CURRENT_TIMESTAMP,
        'WEB MANUAL IMPORT',
        'WEB MANUAL IMPORT')
RETURNING capture_id into v_capture_id;

--insert into entity table
insert into entity
    (organization_id,
    capture_id,
    vc_entity_id,
    entity_name,
    entity_type,
    vc_entity_pid,
    entity_parent_type,
    deleted,
    vcenter_name,
    description,
    is_dummy,
    entity_source)
values( v_organization_id,
        v_capture_id,
        '-1',
        v_identifier,
        'STORAGE',
        '-1',
        'STORAGE_FOLDER',
        'false',
        '',
        v_description,
        'false',
        'Manual WEB Upload'
                )
RETURNING entity_id into v_entity_id;

--insert into storages TABLE
insert into entity_storages
    (entity_id,
    disk_size_capacity_bytes,
    disk_size_provisioned_bytes,
    disk_size_used_bytes,
    dns_name
    )
values(
        v_entity_id,
        (v_capacity_gb::float)*(1024.0*1024.0*1024.0),
        0.0,
        (v_used_gb::float)*(1024.0*1024.0*1024.0),
        v_dns_name
);

--insert into storages_lun_detail table
insert into storages_lun_detail
    (entity_id, "LUN", "date", pool, label)
values(
        v_entity_id,
        v_LUN,
        NOW()::timestamp,
        v_pool,
        v_label);

-- insert into storage_configuration table
insert into storage_configuration
    (entity_id, media_type, storage_type, file_system, used_for, raid_level, capacity_type, usage_values)
values(
        v_entity_id,
        v_media_type,
        v_storage_type,
        v_file_system,
        v_used_for,
        v_RAID,
        v_capacity_type,
        json_build_object('iops', json_build_object(
                            'mean_value', v_mean_iops, 
                            'acceptability', null, 
                            'peak_value', v_peak_iops,
                            'unit', 'ps'),
                            'throughput', json_build_object(
                                'mean_value', v_mean_throughput,
                                'acceptability', null,
                                'peak_value', v_peak_throughput,
                                'unit', 'ms'
                            ),
                            'disk_latency', json_build_object(
                                'mean_value', v_mean_disk_latency,
                                'acceptability', null,
                                'peak_value', v_peak_disk_latency,
                                'unit', 'bps'
                            ),
                            'access_frequency', json_build_object(
                                'acceptability', v_access_frequency
                            )
                        )
                );

return json_build_object(
                        'status','Success',
                        'description', v_entity_id
                );

exception when others then
return json_build_object(
                        'status','Failed',
                        'description',SQLERRM
                );

END;
$function$
;
