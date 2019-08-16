create type storage_type as enum
('SAN','NAS', 'Object');

create type media_type_enum as enum
('HDD', 'SSD');

alter type storage_type add VALUE 'Tape Backup';
alter type media_type_enum add value 'Tape, Object';
  
SELECT enum_range(NULL::storage_type) as storage
union SELECT enum_range(NULL::media_type_enum) as media;

drop type storage_type_enum;

SELECT enum_range(NULL::storage_type) as "Storage Type",
enum_range(NULL::media_type_enum) as "Media Type",
enum_range(NULL::capacity_type_enum) as "Capacity"

//delete enum
DELETE FROM pg_enum WHERE enumlabel = 'Tape Backup' 
AND enumtypid = ( SELECT oid FROM pg_type WHERE typname = 'storage_type');


