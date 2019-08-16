-- Deploy c3analyzerdatabase:merge_split_storage to pg

BEGIN;

-- XXX Add DDLs here.
-- MERGE/SPLIT METHODOLOGY
-- FOR MERGE
-- THERE IS ONE ITEM IN CHILD_ARRAY AND MULTIPLE PARENT IDs
-- GET CONFIGURATION OF INHERITING PARENT ID // PERFORM KEY OPERATION
-- FOR SPLIT
-- THERE IS MULTIPLE ITEMS IN CHILD ARRAY AND ONE PARENT ID // PERFORM INDEXING AT 0
-- GET CONFIGURATION OF PARENT ID
-- COMMON STEPS:
--* INSERT CHILD STORAGES
---------------------------------------------
-- UPSERTING PARENT-CHILD RELATIONSHIP FOR STORAGE (storage_relationship)
-- MERGE: MULTIPLE PARENTS | ONE CHILD (ITERATION OF PARENT)
-- SPLIT: ONE PARENT | MULTIPLE CHILDS (MULTIPLE RETURNS) (ITERATION OF CHILD)

-- MERGE/SPLIT METHODOLOGY
-- FOR MERGE
-- THERE IS ONE ITEM IN CHILD_ARRAY AND MULTIPLE PARENT IDs
-- FOR SPLIT
-- THERE IS MULTIPLE CHILDREN IN CHILD ARRAY AND ONE ITEM IN PARENT IDs
-- COMMON STEPS:
--* INSERT CHILD STORAGES
---------------------------------------------
-- UPSERTING PARENT-CHILD RELATIONSHIP FOR STORAGE (storage_relationship)
-- MERGE: MULTIPLE PARENTS | ONE CHILD (ITERATION OF PARENT)
-- SPLIT: ONE PARENT | MULTIPLE CHILDS (MULTIPLE RETURNS) (ITERATION OF CHILD)

CREATE OR REPLACE FUNCTION "public"."merge_split_test"("storage_action" varchar, "parent_ids" int[], "child_array" json, 
    "excluded_name" text default '')
  RETURNS "pg_catalog"."json" AS $BODY$

DECLARE
    parent_config json;
    v_organization_id text;

    parent_count int;
    child_count int;

    child json;
    child_id int;

    duplicate_children_input text;
    duplicate_children_warehouse text;

    return_result json;
    success_storage_array int[];

    -- FOR ERROR DETAILED DESCRIPTION
    _c text;

BEGIN

-- GETTING PARENT AND CHILD COUNT PASSED FROM F.E.
SELECT array_length(parent_ids, 1) INTO parent_count;
SELECT json_array_length(child_array) INTO child_count;
SELECT (child_array->>0)::json->>'organization_id' INTO v_organization_id;

-- VALIDATION STACK
--@1 CHECKS IF storage_actions ARE VALID
IF (storage_action <> 'merge') and (storage_action <> 'split') THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT(storage_action, ' not allowed')
    );
END IF;
--@2 CHECKS IF PARENT IS PRESENT
IF (parent_count <= 0) THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT('No parent(s) provided for ', storage_action)
    );
END IF;
--@3 CHECKS IF CHILD IS PRESENT
IF (child_count <= 0) THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT('No child(ren) information provided for ', storage_action)
    );
END IF;
--@4 CHECKS IF PARENTs COUNT IS GREATER THAN 1 FOR MERGE
IF storage_action = 'merge' and (parent_count <= 1) THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT('More than ',parent_count,' parents required for ', storage_action)
    );
END IF;
--@5 CHECKS IF CHILDREN COUNT IS GREATER THAN 1 FOR SPLITS
IF storage_action = 'split' and (child_count <= 1) THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT('More than ',child_count,' children required for ', storage_action)
    );
END IF;
--@6 CHECKS IF CHILDREN NAME IS DUPLICATE IN INPUT
WITH cte as (
SELECT 
identifier
from json_to_recordset(child_array)
as x("identifier" text)
)
SELECT string_agg(identifier, ',') FROM (
SELECT DISTINCT identifier FROM cte 
GROUP BY identifier HAVING count(identifier) > 1) x 
INTO duplicate_children_input;
IF duplicate_children_input IS NOT NULL THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT('Duplicate identifier(s) in input'),
    'identifier', duplicate_children_input);
END IF;
--@7 CHECKS IF CHILDREN NAME IS ALREADY PRESENT IN WAREHOUSE
SELECT string_agg(identifier, ',') FROM v_entity_storages 
WHERE
deleted='f' and
is_duplicate_of IS NULL and
organization_id=v_organization_id and
identifier <> excluded_name and
identifier in (
SELECT 
identifier
FROM json_to_recordset(child_array)
AS x("identifier" text)) INTO duplicate_children_warehouse;
IF duplicate_children_warehouse IS NOT NULL THEN
return json_build_object(
    'status','Failed',
    'description',CONCAT('Identifier(s) already present in warehouse'), 
    'identifier', duplicate_children_warehouse);
END IF;

-- INSERT INTO CCANWAREHOUSE
-- EACH CHILD STORAGE WILL BE INSERTED WITH ITERATION
FOR child IN SELECT * FROM json_array_elements(child_array) LOOP
    SELECT * FROM "public".add_storage(child::json, excluded_name) INTO return_result;
    
    -- APPENDING array into storage
    SELECT array_append(success_storage_array, (return_result ->> 'description')::int) 
    INTO success_storage_array;

    -- WHAT IF THERE OCCURS ERROR IN Nth ITERATION AFTER N SUCCESSFUL INSERTS
    -- CONTINGENCY REVERT OF EVERYTHING THAT'S BEEN INSERTED
    -- INCASE OF FAILURE, RUN CONTINGENCY PROCEDURE
    -- COMMENT IF NECESSARY
    
    -- COMMENTABLE BLOCK STARTS
    IF return_result ->> 'status' = 'Failed' THEN
        FOREACH child_id in ARRAY success_storage_array LOOP
            SELECT * FROM "public".revert_merge_split_storage(child::int);
        END LOOP;
        return json_build_object(
            'status','Failed',
            'description',CONCAT('Failure while ',storage_action,'. Action reverted')
            );
    END IF;
    -- COMMENTABLE BLOCK ENDS
	
END LOOP;

INSERT INTO storage_relationship(entity_id, parent_id)
SELECT unnest(success_storage_array) entity_id,
unnest (parent_ids) parent_id;


return json_build_object(
                        'status','Success',
                        'description', CONCAT(storage_action, ' completed successfully'),
                        'entity_ids', success_storage_array
                );

exception when others then
GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
return json_build_object(
                        'status','Failed',
                        'description',CONCAT(SQLERRM, ',', _c)
                );

END;
$BODY$
  LANGUAGE 'plpgsql' VOLATILE COST 100
;

COMMIT;
