from simple_salesforce import Salesforce
import pandas as pd
from dotenv import load_dotenv
from datetime import date
from r2r_pipelines import export_db
import os

def fetch_and_store_sf_opportunities():
    # Load environment variables
    load_dotenv(override=True)

    USERNAME = os.getenv("SF_USERNAME")
    PASSWORD = os.getenv("SF_PASSWORD")
    SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")

    # Connect to Salesforce
    sf = Salesforce(username=USERNAME, password=PASSWORD, security_token=SECURITY_TOKEN)

    # Query Salesforce
    query = sf.query_all("""
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 84760f9 (n))
    select 
        Opportunity_ID_Report__c,
        RecordType.Name,
        Market_Segment__c,
        Owner_s_Role__c,
        Owner.Name,
        Co_owner__c
    from Opportunity 
    where 
        Programme_Intake_Year__c >= '2025'
        and Pre_Enrolled_Date__c != null
        and StageName in ('Pre-Enrolled','Enrolled','Pre-registered','Registered')
        and (NOT (Programme_Status__c like '%Transfer%' or Programme_Status__c like '%transfer%'))
        and (NOT (Programme_Status__c like '%Deferred (Intake)%'))
        and Cancelled_rejected__c = false
<<<<<<< HEAD
=======
        SELECT Opportunity_ID_Report__c, RecordType.Name, Market_Segment__c, Owner_s_Role__c, Owner.Name,
               Name, StageName, Intake_Year__c, Intake_Month__c, Campus_Preference_1__c, Level_1__c,
               Vertical_1__c, Programme_1__c, Programme_Code__c, Programme_Name__c, School__r.Name,
               Institution__r.Name, Programme_Intake_Year__c, Programme_Intake_Month__c,
               Programme_Status_Staging__c, Last_Programme_Status_Update__c,
               Admission_Response_Status_Staging__c, Student_Email_Programme_Name__c,
               Account_ID_Report__c, Account.Name, I_C_Number__c, Passport_Number__c, Race__c,
               Nationality__c, Account.gender__c, LeadSource, Web_Source_Group__c, Pre_Enrolled_Date__c,
               Registered_Date__c, Account.PersonMailingCountry, State__c, Entry_Qualification__c,
               UG_PG_TC__c, ENR_Check__c, Bursary_Group__c, Bursary_Deduction__c,
               Scholarship_Deduction__c, Withdrawn_Pre_commencement_Sales_Track__c,
               Programme_Status_Previous_Record__c, IPT_Note__c, Stage_Previous_Record__c,
               Intake_Year_Previous_Record__c, Intake_Month_Previous_Record__c, Region_with_Africa__c,
               Region_w_o_africa__c, Online_Source__c, WEB_SOURCE_GRP__c, CreatedDate, CYCLE__c,
               Campaign.Name, Campaign.RecordType.Name, Campaign.Type, Campaign.Campaign_Source__c,
               Campaign.Campaign_Cycle__c, Campaign.Campaign_Subtype__c, Campaign.Organiser__c,
               Campaign.Partner_School_Account__r.Name, Campaign.StartDate, Campaign.EndDate,
               Campaign.Status, Campaign.IsActive, Last_Modified_DateTime__c, Withdrawal_Date__c,
               UTM_Campaign__c, UTM_Campaign_Last_Touch__c, UTM_Content__c, UTM_Content_Last_Touch__c,
               UTM_Medium__c, UTM_Medium_Last_Touch__c, UTM_Source__c, UTM_Source_Last_Touch__c,
               UTM_Term__c, UTM_Term_Last_Touch__c, ROLE__c, Programme_2__c, Account.PersonMailingCity,
               Agent_s_State__c, Agent__r.Name, Agent_s_City__c, Enrolled_by_agent__c,
               Commission_Amount_Forecast_RM__c, MICPA_Resit_student__c, MICPA_CAANZ_Count__c,
               MICPA_CAANZ_module__c, ACCA_Module_Count__c, ACCA_Module__c, Student_s_Result__c,
               Subject_Stream_CAL_SACEi__c, Stipend_1st_Yr_Allocation_RM__c,
               Programme_Name_Previous_Record__c, Institution_others__c, School_Others__c,
               Corporate_Sponsorship_Source__c, Sponsorship_Code__c, Sponsorship_Description__c,
               Corporate_Partnership_Source__c, Institutional_Partnership__c, Partner_School_Source__c,
               Partnership_Body__c, Web_Source__c, Student_s_Current_Age__c,
               Postgraduate_Mode_of_Study__c, Job_Title__c, Job_Industry__c, Years_of_Experience__c,
               Orientation_Date__c, Intake_Closing_Date__c, Bursary_Deduction_2__c,
               Account.LastActivityDate, VISA_Status__c, Remarks_VISA__c, VISA_Status_Last_Updated__c,
               VISA_application_status__c, Non_REG_Remark_2__c, Non_REG_Manager_Remark__c,
               Non_REG_Last_Updated__c, Adjusted_Conversion_Rate__c, Non_REG_Status__c,
               Non_REG_Remark_1__c, Referral_Agent__c
        FROM Opportunity 
        WHERE Programme_Intake_Year__c >= '2025'
          AND Pre_Enrolled_Date__c != null
          AND StageName IN ('Pre-Enrolled', 'Enrolled', 'Pre-registered', 'Registered')
          AND Programme_Status__c NOT LIKE '%Transfer%'
          AND Programme_Status__c NOT LIKE '%transfer%'
          AND Programme_Status__c NOT LIKE '%Deferred (Intake)%'
          AND Cancelled_rejected__c = false
>>>>>>> 3e814c0 (adding from server)
=======
>>>>>>> 84760f9 (n))
    """)

    # Helper function to flatten nested Salesforce records
    def flatten_sf_record(record, parent_key='', sep='_'):
        items = []
        for k, v in record.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if new_key in ['attributes_type', 'attributes_url']:
                continue  # skip metadata
            if isinstance(v, dict):
                items.extend(flatten_sf_record(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    # Flatten and load into DataFrame
    flattened_records = [flatten_sf_record(rec) for rec in query['records']]
    df = pd.DataFrame(flattened_records)

    # Add import date
    df['date_import'] = date.today()

    # Get database engine
    engine = export_db.marcommdb_connection()

    # Export to SQL
    df.to_sql('fact_daily_enreg', engine, schema='staging', if_exists='append', index=False)

    print(f"Inserted {len(df)} records into 'daily_enreg' table.")

