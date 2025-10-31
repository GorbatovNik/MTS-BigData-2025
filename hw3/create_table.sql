SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- -----------------------------------------------------------------
-- 1. Целевая партиционированная таблица (registration_date - ключ)
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS test.some_data
(
    registration_number VARCHAR(1000),
    registration_date VARCHAR(1000),
    application_number VARCHAR(1000),
    application_date VARCHAR(1000),
    priority_date VARCHAR(1000),
    exhibition_priority_date VARCHAR(1000),
    paris_convention_priority_number VARCHAR(1000),
    paris_convention_priority_date VARCHAR(1000),
    paris_convention_priority_country_code VARCHAR(1000),
    initial_application_number VARCHAR(1000),
    initial_application_priority_date VARCHAR(1000),
    initial_registration_number VARCHAR(1000),
    initial_registration_date VARCHAR(1000),
    international_registration_number VARCHAR(1000),
    international_registration_date VARCHAR(1000),
    international_registration_priority_date VARCHAR(1000),
    international_registration_entry_date VARCHAR(1000),
    application_number_for_recognition_of_trademark_from_Crimea VARCHAR(1000),
    application_date_for_recognition_of_trademark_from_Crimea VARCHAR(1000),
    Crimean_trademark_application_number_for_state_registration_in_Ukraine VARCHAR(1000),
    Crimean_trademark_application_date_for_state_registration_in_Ukraine VARCHAR(1000),
    Crimean_trademark_certificate_number_in_Ukraine VARCHAR(1000),
    exclusive_rights_transfer_agreement_registration_number VARCHAR(1000),
    exclusive_rights_transfer_agreement_registration_date VARCHAR(1000),
    legally_related_applications VARCHAR(1000),
    legally_related_registrations VARCHAR(1000),
    expiration_date VARCHAR(1000),
    right_holder_name VARCHAR(1000),
    foreign_right_holder_name VARCHAR(1000),
    right_holder_address VARCHAR(1000),
    right_holder_ogrn VARCHAR(1000),
    right_holder_inn VARCHAR(1000),
    correspondence_address VARCHAR(1000),
    collective VARCHAR(1000),
    collective_users VARCHAR(1000),
    extraction_from_charter_of_the_collective_trademark VARCHAR(1000),
    color_specification VARCHAR(1000),
    unprotected_elements VARCHAR(1000),
    kind_specification VARCHAR(1000),
    threedimensional VARCHAR(1000),
    threedimensional_specification VARCHAR(1000),
    holographic VARCHAR(1000),
    holographic_specification VARCHAR(1000),
    sound VARCHAR(1000),
    sound_specification VARCHAR(1000),
    olfactory VARCHAR(1000),
    olfactory_specification VARCHAR(1000),
    color VARCHAR(1000),
    color_trademark_specification VARCHAR(1000),
    light VARCHAR(1000),
    light_specification VARCHAR(1000),
    changing VARCHAR(1000),
    changing_specification VARCHAR(1000),
    positional VARCHAR(1000),
    positional_specification VARCHAR(1000),
    actual VARCHAR(1000),
    publication VARCHAR(1000)
)
PARTITIONED BY (
    right_holder_country_code VARCHAR(1000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- -----------------------------------------------------------------
-- 2. Временная Staging-таблица (содержит ВСЕ поля из CSV)
-- -----------------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS test.some_data_staging
(
    registration_number VARCHAR(1000),
    registration_date VARCHAR(1000),
    application_number VARCHAR(1000),
    application_date VARCHAR(1000),
    priority_date VARCHAR(1000),
    exhibition_priority_date VARCHAR(1000),
    paris_convention_priority_number VARCHAR(1000),
    paris_convention_priority_date VARCHAR(1000),
    paris_convention_priority_country_code VARCHAR(1000),
    initial_application_number VARCHAR(1000),
    initial_application_priority_date VARCHAR(1000),
    initial_registration_number VARCHAR(1000),
    initial_registration_date VARCHAR(1000),
    international_registration_number VARCHAR(1000),
    international_registration_date VARCHAR(1000),
    international_registration_priority_date VARCHAR(1000),
    international_registration_entry_date VARCHAR(1000),
    application_number_for_recognition_of_trademark_from_Crimea VARCHAR(1000),
    application_date_for_recognition_of_trademark_from_Crimea VARCHAR(1000),
    Crimean_trademark_application_number_for_state_registration_in_Ukraine VARCHAR(1000),
    Crimean_trademark_application_date_for_state_registration_in_Ukraine VARCHAR(1000),
    Crimean_trademark_certificate_number_in_Ukraine VARCHAR(1000),
    exclusive_rights_transfer_agreement_registration_number VARCHAR(1000),
    exclusive_rights_transfer_agreement_registration_date VARCHAR(1000),
    legally_related_applications VARCHAR(1000),
    legally_related_registrations VARCHAR(1000),
    expiration_date VARCHAR(1000),
    right_holder_name VARCHAR(1000),
    foreign_right_holder_name VARCHAR(1000),
    right_holder_address VARCHAR(1000),
    right_holder_country_code VARCHAR(1000),
    right_holder_ogrn VARCHAR(1000),
    right_holder_inn VARCHAR(1000),
    correspondence_address VARCHAR(1000),
    collective VARCHAR(1000),
    collective_users VARCHAR(1000),
    extraction_from_charter_of_the_collective_trademark VARCHAR(1000),
    color_specification VARCHAR(1000),
    unprotected_elements VARCHAR(1000),
    kind_specification VARCHAR(1000),
    threedimensional VARCHAR(1000),
    threedimensional_specification VARCHAR(1000),
    holographic VARCHAR(1000),
    holographic_specification VARCHAR(1000),
    sound VARCHAR(1000),
    sound_specification VARCHAR(1000),
    olfactory VARCHAR(1000),
    olfactory_specification VARCHAR(1000),
    color VARCHAR(1000),
    color_trademark_specification VARCHAR(1000),
    light VARCHAR(1000),
    light_specification VARCHAR(1000),
    changing VARCHAR(1000),
    changing_specification VARCHAR(1000),
    positional VARCHAR(1000),
    positional_specification VARCHAR(1000),
    actual VARCHAR(1000),
    publication VARCHAR(1000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ('skip.header.line.count'='1');

-- -----------------------------------------------------------------
-- 3. Загрузка данных в Staging-таблицу
-- -----------------------------------------------------------------
LOAD DATA INPATH '/input/${DATA_NAME}' INTO TABLE test.some_data_staging;

-- -----------------------------------------------------------------
-- 4. Вставка данных из Staging в целевую партиционированную таблицу
-- -----------------------------------------------------------------
INSERT OVERWRITE TABLE test.some_data PARTITION(right_holder_country_code)
SELECT
    registration_number, registration_date, application_number, application_date,
    priority_date, exhibition_priority_date, paris_convention_priority_number,
    paris_convention_priority_date, paris_convention_priority_country_code,
    initial_application_number, initial_application_priority_date,
    initial_registration_number, initial_registration_date,
    international_registration_number, international_registration_date,
    international_registration_priority_date, international_registration_entry_date,
    application_number_for_recognition_of_trademark_from_Crimea,
    application_date_for_recognition_of_trademark_from_Crimea,
    Crimean_trademark_application_number_for_state_registration_in_Ukraine,
    Crimean_trademark_application_date_for_state_registration_in_Ukraine,
    Crimean_trademark_certificate_number_in_Ukraine,
    exclusive_rights_transfer_agreement_registration_number,
    exclusive_rights_transfer_agreement_registration_date,
    legally_related_applications, legally_related_registrations,
    expiration_date, right_holder_name, foreign_right_holder_name,
    right_holder_address, right_holder_ogrn,
    right_holder_inn, correspondence_address, collective,
    collective_users, extraction_from_charter_of_the_collective_trademark,
    color_specification, unprotected_elements, kind_specification,
    threedimensional, threedimensional_specification, holographic,
    holographic_specification, sound, sound_specification,
    olfactory, olfactory_specification, color,
    color_trademark_specification, light, light_specification,
    changing, changing_specification, positional,
    positional_specification, actual, publication,
    right_holder_country_code
FROM test.some_data_staging
LIMIT 1000;

DROP TABLE test.some_data_staging;