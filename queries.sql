with source as (

    select * from raw_patients

),

renamed as (
    
    select
        id as patient,
        try_cast(birthdate as date) as birth_date,
        try_cast(deathdate as date) as death_date,
        case
            when death_date is null then year(age(birth_date))
            else year(age(death_date, birth_date))
            end as age,
        ssn,
        drivers as drivers_license_no,
        passport as passport_no,
        first as first_name,
        last as last_name,
        marital as marital_status,
        race,
        ethnicity,
        gender,
        address,
        city,
        state,
        zip,
        county,
        lon,
        lat,
        round(healthcare_expenses, 2) as healthcare_expenses,
        round(healthcare_coverage, 2) as healthcare_coverage

    from source
    qualify row_number() over (partition by id) = 1

)

select * from renamed




with source as (

    select * from raw_encounters

),

renamed as (

    select
        id as encounter,
        start as encounter_start_time,
        stop as encounter_stop_time,
        patient,
        organization,
        provider,
        payer as encounter_payer,
        payer_coverage as encounter_payer_coverage,
        encounterclass as encounter_class,
        code as encounter_code,
        description as encounter_description,
        base_encounter_cost,
        total_claim_cost as total_encounter_cost,
        reasoncode as encounter_diag_code,
        reasondescription as encounter_diag_description

    from source
    qualify row_number() over (partition by id) = 1

)

select * from renamed


















with source as (
    
    select * from raw_medications

),

renamed as (

    select
        start as medication_start_time,
        stop as medication_end_time,
        patient,
        payer as medication_payer,
        encounter as medication_encounter,
        code as medication_code,
        description as medication_description,
        dispenses,
        payer_coverage as medication_payer_coverage,
        base_cost as base_medication_cost,
        totalcost as total_medication_cost,
        reasoncode as medication_diag_code,
        reasondescription as medication_diag_description

    from source
    qualify row_number() over (partition by encounter) = 1
)

select * from renamed



















with patients as (

    select * from {{ ref('stg_patients') }}

),

medications as (

    select * from {{ ref('stg_medications') }}

),

encounters as (

    select * from {{ ref('stg_encounters') }}

),

patient_encounters as (

    select
        patients.patient,
        encounters.encounter,
        encounters.encounter_start_time,
        encounters.encounter_stop_time,
        encounters.encounter_class,
        encounters.encounter_diag_code,
        encounters.encounter_diag_description,
        encounters.base_encounter_cost,
        encounters.total_encounter_cost,
        encounters.encounter_payer,
        encounters.encounter_payer_coverage,
    
    from patients
    
    left join encounters
        on patients.patient = encounters.patient

),

patient_medications as (

    select
        patients.patient,
        medications.medication_encounter,
        medications.medication_start_time,
        medications.medication_end_time,
        medications.medication_code,
        medications.medication_description,
        medications.medication_diag_code,
        medications.medication_diag_description,
        medications.dispenses,
        medications.medication_payer,
        medications.medication_payer_coverage,
        medications.base_medication_cost,
        medications.total_medication_cost,

    from patients

    left join medications
        on patients.patient = medications.patient

),

final as (

    select
        patients.patient,
        patients.first_name,
        patients.last_name,
        patients.birth_date,
        patients.death_date,
        patients.age,
        patients.healthcare_expenses,
        patients.healthcare_coverage,
        patient_encounters.encounter,
        patient_encounters.encounter_start_time,
        patient_encounters.encounter_stop_time,
        patient_encounters.encounter_class,
        patient_encounters.encounter_diag_code,
        patient_encounters.encounter_diag_description,
        patient_encounters.base_encounter_cost,
        patient_encounters.total_encounter_cost,
        patient_encounters.encounter_payer,
        patient_encounters.encounter_payer_coverage,
        patient_medications.medication_start_time,
        patient_medications.medication_end_time,
        patient_medications.medication_code,
        patient_medications.medication_description,
        patient_medications.medication_diag_code,
        patient_medications.medication_diag_description,
        patient_medications.dispenses,
        patient_medications.medication_payer,
        patient_medications.medication_payer_coverage,
        patient_medications.base_medication_cost,
        patient_medications.total_medication_cost

    from patients

    left join patient_encounters
        on patients.patient = patient_encounters.patient

    left join patient_medications
        on patients.patient = patient_medications.patient
        and patient_encounters.encounter = patient_medications.medication_encounter

)

select * from final
