
# Predict Query
/*
This query takes the model created in the previous query and uses it to score new users.
The date range for the predictions is after the date range of the training dataset.
The output is a record for every fullVisitorId ranked by conversion probability, with an assigned
quintile (1 = most likely, 5 = least likely).
This can be used for remarketing audience segmentation.
*/

# Set Parameters
DECLARE session_start, session_end, country STRING;
DECLARE lookback INT64;
SET session_start = '20170501';
SET session_end = '20170801';
SET country = 'United States';
SET lookback = 30;  # Sets Lookback Window e.g. include session data that occured X days before last session for non-buyers and last conversion session for buyers

WITH new_data AS(

# Label buyers and non buyers (1 and 0) and create timestamps for filtering criteria
WITH users_labeled as ( 
SELECT 
    fullVisitorId, 
    MAX(case when totals.transactions >= 1 then 1 else 0 end) as labels,
    MIN(visitStartTime) as first_visit,
    MIN(case when totals.transactions >= 1 then visitStartTime end) as first_conversion_session,
    MAX(case when totals.transactions >=1 then PARSE_TIMESTAMP('%s', CAST(visitStartTime as string)) end) as last_conversion_timestamp,
    MAX(PARSE_TIMESTAMP('%s', CAST(visitStartTime as string))) as last_visit_timestamp
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE 
    _TABLE_SUFFIX BETWEEN session_start AND session_end
    AND geoNetwork.Country=country
    AND totals.bounces is null
GROUP BY 1
)
,

# Create field that contains last_conversion_timestamp for converters or last_visit_timestamp for non-buyers
users_labeled_timestamp as (
SELECT
    *,
    CASE WHEN labels = 1 THEN last_conversion_timestamp ELSE last_visit_timestamp END AS last_timestamp
FROM users_labeled
)
,

# Create lookback window field used later on to filter sessions for those that fall within window.
# Remove users that converted in first visit

filtered_users_labeled as (
SELECT
    fullVisitorId,
    labels,
    first_conversion_session,
    last_visit_timestamp,
    last_conversion_timestamp,
    TIMESTAMP_SUB(last_timestamp, INTERVAL lookback DAY) AS front_lookback_window, # Returns timestamp X days before last visit (non-buyers) or last conversion (buyers)
    last_timestamp as end_lookback_window
FROM users_labeled_timestamp
WHERE
    labels = 0 OR (labels = 1 AND (first_visit < first_conversion_session OR first_conversion_session IS NULL)) # Removes users who converted in first session
)
,

# Summary Metrics
summary_table AS (
SELECT
    fullVisitorId,
    SUM(totals.timeOnSite) / SUM(totals.visits) AS timeOnSite_per_visit,
    SUM(totals.timeOnSite) / SUM(totals.pageviews) AS timeOnSite_per_page,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE 
    _TABLE_SUFFIX BETWEEN session_start AND session_end
    AND geoNetwork.Country=country
GROUP BY 1
)
,

# User Session Metrics
user_sessions AS (
SELECT 
a.fullVisitorId,

# Device Dimensions
## Category
MAX(CASE WHEN device.deviceCategory = "mobile" THEN 1 ELSE 0 END) AS device_deviceCategory_mobile, ### high neg corr with desktop
MAX(CASE WHEN device.deviceCategory = "desktop" THEN 1 ELSE 0 END) AS device_deviceCategory_desktop, ### high corr with android and ios
MAX(CASE WHEN device.deviceCategory = "tablet" THEN 1 ELSE 0 END) AS device_deviceCategory_tablet,

## Browser
MAX(CASE WHEN device.browser = "Chrome" THEN 1 ELSE 0 END) AS device_browser_Chrome, ### neg corr with safari
MAX(CASE WHEN device.browser = "Safari" THEN 1 ELSE 0 END) AS device_browser_Safari, ### high corr with ios
MAX(CASE WHEN device.browser = "Firefox" THEN 1 ELSE 0 END) AS device_browser_Firefox,
MAX(CASE WHEN device.browser = "Edge" THEN 1 ELSE 0 END) AS device_browser_Edge,
 
## OS
-- MAX(CASE WHEN device.operatingSystem = "iOS" THEN 1 ELSE 0 END) AS device_operatingSystem_iOS,
-- MAX(CASE WHEN device.operatingSystem = "Android" THEN 1 ELSE 0 END) AS device_operatingSystem_Android,
-- MAX(CASE WHEN device.operatingSystem = "Windows" THEN 1 ELSE 0 END) AS device_operatingSystem_Windows,
-- MAX(CASE WHEN device.operatingSystem = "Macintosh" THEN 1 ELSE 0 END) AS device_operatingSystem_Macintosh,
-- MAX(CASE WHEN device.operatingSystem = "Chrome OS" THEN 1 ELSE 0 END) AS device_operatingSystem_Chrome_OS,

## Device Brand -- not available in demo dataset
-- MAX(CASE WHEN device.mobileDeviceBranding = "Apple" THEN 1 ELSE 0 END) AS device_mobileDeviceBranding_Apple,
-- MAX(CASE WHEN device.mobileDeviceBranding = "Samsung" THEN 1 ELSE 0 END) AS device_mobileDeviceBranding_Samsung,
-- MAX(CASE WHEN device.mobileDeviceBranding = "LG" THEN 1 ELSE 0 END) AS device_mobileDeviceBranding_LG,
-- MAX(CASE WHEN device.mobileDeviceBranding = "Motorola" THEN 1 ELSE 0 END) AS device_mobileDeviceBranding_Motorola,

# Traffic Source Dimensions
## isTrueDirect -- overlap with channel grouping
-- MAX(CASE WHEN trafficSource.isTrueDirect = True THEN 1 ELSE 0 END) AS trafficSource_isTrueDirect_True,

## Medium -- overlap with channel grouping
-- MAX(CASE WHEN trafficSource.medium = "affiliate" THEN 1 ELSE 0 END) AS trafficSource_medium_affiliate,
-- MAX(CASE WHEN trafficSource.medium = "referral" THEN 1 ELSE 0 END) AS trafficSource_medium_referral,
-- MAX(CASE WHEN trafficSource.medium = "organic" THEN 1 ELSE 0 END) AS trafficSource_medium_organic,
-- MAX(CASE WHEN trafficSource.medium = "cpc" THEN 1 ELSE 0 END) AS trafficSource_medium_cpc, # click per cost (paid search)
-- MAX(CASE WHEN trafficSource.medium = "cpm" THEN 1 ELSE 0 END) AS trafficSource_medium_cpm, # display
-- MAX(CASE WHEN trafficSource.medium = "(non set)" THEN 1 ELSE 0 END) AS trafficSource_medium_not_set,
-- MAX(CASE WHEN trafficSource.medium = "(none)" THEN 1 ELSE 0 END) AS trafficSource_medium_none,

## Channel Grouping
MAX(CASE WHEN channelGrouping = "Display" THEN 1 ELSE 0 END) AS channelGrouping_Display,
MAX(CASE WHEN channelGrouping = "Organic Search" THEN 1 ELSE 0 END) AS channelGrouping_OrganicSearch,
MAX(CASE WHEN channelGrouping = "Social" THEN 1 ELSE 0 END) AS channelGrouping_Social,
MAX(CASE WHEN channelGrouping = "Direct" THEN 1 ELSE 0 END) AS channelGrouping_Direct,
MAX(CASE WHEN channelGrouping = "Referral" THEN 1 ELSE 0 END) AS channelGrouping_Referal,
MAX(CASE WHEN channelGrouping = "Paid Search" THEN 1 ELSE 0 END) AS channelGrouping_Paid_Search,
MAX(CASE WHEN channelGrouping = "(Other)" THEN 1 ELSE 0 END) AS channelGrouping_Other,
MAX(CASE WHEN channelGrouping = "Affiliates" THEN 1 ELSE 0 END) AS channelGrouping_Affiliates,

# Page
## Page Level 1
-- MAX(CASE WHEN hits.page.pagePathLevel1 = "/signin.html" THEN 1 ELSE 0 END) AS hitspagepagePathLevel1_signin, ### data leak with label

## Page Level 2
MAX(CASE WHEN hits.page.pagePathLevel2 = "/brands/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_brands,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/quickview" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_quickview,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/lifestyle" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_lifestyle,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/lifestyle/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_lifestyle2,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/return-policy" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_returnpolicy,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/terms-of-use/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_termsofuse,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/faqs" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_faqs,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/frequently-asked-questions/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_faq,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/shop+by+brand" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_shopbybrand,
MAX(CASE WHEN hits.page.pagePathLevel2 = "/shop+by+brand/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel2_shopbybrand2,

## Page Level 3 -- can add more
MAX(CASE WHEN hits.page.pagePathLevel3 = "/men" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_men,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/mens" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_mens,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/mens/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_mens2,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/womens" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_womens,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/womens/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_womens2,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/quickview" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_quickview,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/return-policy" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_returnpolicy,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/store-policies/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_storepolicies,
MAX(CASE WHEN hits.page.pagePathLevel3 = "/frequently-asked-questions/" THEN 1 ELSE 0 END) AS hitspagepagePathLevel3_faq,

# Geo
## Metro
-- Max(CASE WHEN geoNetwork.metro='(not set)' OR geoNetwork.metro='New York NY' THEN 1 ELSE 0 END) AS geoNetworkmetro_notset, ### high corr with new york, abilene, albany
-- Max(CASE WHEN geoNetwork.metro='Abilene-Sweetwater TX' OR geoNetwork.metro='New York NY' THEN 1 ELSE 0 END) AS geoNetworkmetro_AbileneSweetwaterTX, ### high corr with new york
Max(CASE WHEN geoNetwork.metro='Albany-Schenectady-Troy NY' THEN 1 ELSE 0 END) AS geoNetworkmetro_AlbanySchenectadyTroyNY,
Max(CASE WHEN geoNetwork.metro='Atlanta GA' THEN 1 ELSE 0 END) AS geoNetworkmetro_AtlantaGA,
Max(CASE WHEN geoNetwork.metro='Augusta GA' THEN 1 ELSE 0 END) AS geoNetworkmetro_AugustaGA,
Max(CASE WHEN geoNetwork.metro='Austin TX' THEN 1 ELSE 0 END) AS geoNetworkmetro_AustinTX,
Max(CASE WHEN geoNetwork.metro='Baltimore MD' THEN 1 ELSE 0 END) AS geoNetworkmetro_BaltimoreMD,
Max(CASE WHEN geoNetwork.metro='Boise ID' THEN 1 ELSE 0 END) AS geoNetworkmetro_BoiseID,
Max(CASE WHEN geoNetwork.metro='Boston MA-Manchester NH' THEN 1 ELSE 0 END) AS geoNetworkmetro_BostonMAManchesterNH,
Max(CASE WHEN geoNetwork.metro='Butte-Bozeman MT' THEN 1 ELSE 0 END) AS geoNetworkmetro_ButteBozemannMt,
Max(CASE WHEN geoNetwork.metro='Charleston SC' THEN 1 ELSE 0 END) AS geoNetworkmetro_CharlestonSC,
Max(CASE WHEN geoNetwork.metro='Charlotte NC' THEN 1 ELSE 0 END) AS geoNetworkmetro_CharlotteNC,
Max(CASE WHEN geoNetwork.metro='Charlottesville VA' THEN 1 ELSE 0 END) AS geoNetworkmetro_CharlottesvilleVA,
Max(CASE WHEN geoNetwork.metro='Chattanooga TN' THEN 1 ELSE 0 END) AS geoNetworkmetro_ChattanoogaTN,
Max(CASE WHEN geoNetwork.metro='Chicago IL' THEN 1 ELSE 0 END) AS geoNetworkmetro_ChicagoIL,
Max(CASE WHEN geoNetwork.metro='Chico-Redding CA' THEN 1 ELSE 0 END) AS geoNetworkmetro_ChicoReddingCA,
Max(CASE WHEN geoNetwork.metro='Cincinnati OH' THEN 1 ELSE 0 END) AS geoNetworkmetro_CincinnatiOH,
Max(CASE WHEN geoNetwork.metro='Cleveland-Akron (Canton) OH' THEN 1 ELSE 0 END) AS geoNetworkmetro_ClevelandAkronCantonOH,
Max(CASE WHEN geoNetwork.metro='Colorado Springs-Pueblo CO' THEN 1 ELSE 0 END) AS geoNetworkmetro_ColoradoSpringsPuebloCO,
Max(CASE WHEN geoNetwork.metro='Columbus OH' THEN 1 ELSE 0 END) AS geoNetworkmetro_ColumbusOH,
Max(CASE WHEN geoNetwork.metro='Dallas-Ft. Worth TX' THEN 1 ELSE 0 END) AS geoNetworkmetro_DallasFtWorthTX,
Max(CASE WHEN geoNetwork.metro='Denver CO' THEN 1 ELSE 0 END) AS geoNetworkmetro_DenverCO,
Max(CASE WHEN geoNetwork.metro='Detroit MI' THEN 1 ELSE 0 END) AS geoNetworkmetro_DetroitMI,
Max(CASE WHEN geoNetwork.metro='El Paso TX' THEN 1 ELSE 0 END) AS geoNetworkmetro_ElPasoTX,
Max(CASE WHEN geoNetwork.metro='Fresno-Visalia CA' THEN 1 ELSE 0 END) AS geoNetworkmetro_FresnoVisaliaCA,
Max(CASE WHEN geoNetwork.metro='Grand Rapids-Kalamazoo-Battle Creek MI' THEN 1 ELSE 0 END) AS geoNetworkmetro_GrandRapidsKalamazooBattleCreekMI,
Max(CASE WHEN geoNetwork.metro='Green Bay-Appleton WI' THEN 1 ELSE 0 END) AS geoNetworkmetro_GreenBayAppletonWI,
Max(CASE WHEN geoNetwork.metro='Harlingen-Weslaco-Brownsville-McAllen TX' THEN 1 ELSE 0 END) AS geoNetworkmetro_HarlingenWeslacoBrownsvilleMcAllenTX,
Max(CASE WHEN geoNetwork.metro='Hartford & New Haven CT' THEN 1 ELSE 0 END) AS geoNetworkmetro_HartfordNewHavenCT,
Max(CASE WHEN geoNetwork.metro='Honolulu HI' THEN 1 ELSE 0 END) AS geoNetworkmetro_HonoluluHI,
Max(CASE WHEN geoNetwork.metro='Houston TX' THEN 1 ELSE 0 END) AS geoNetworkmetro_HoustonTX,
Max(CASE WHEN geoNetwork.metro='Idaho Falls-Pocatello ID' THEN 1 ELSE 0 END) AS geoNetworkmetro_OdahoFallsPocatelloID,
Max(CASE WHEN geoNetwork.metro='Indianapolis IN' THEN 1 ELSE 0 END) AS geoNetworkmetro_IndianapolisIN,
Max(CASE WHEN geoNetwork.metro='Jacksonville FL' THEN 1 ELSE 0 END) AS geoNetworkmetro_JacksonvilleFL,
Max(CASE WHEN geoNetwork.metro='Kansas City MO' THEN 1 ELSE 0 END) AS geoNetworkmetro_KansasCityMO,
Max(CASE WHEN geoNetwork.metro='La Crosse-Eau Claire WI' THEN 1 ELSE 0 END) AS geoNetworkmetro_LaCrosseEauClaireWI,
Max(CASE WHEN geoNetwork.metro='Lansing MI' THEN 1 ELSE 0 END) AS geoNetworkmetro_LansingMI,
Max(CASE WHEN geoNetwork.metro='Las Vegas NV' THEN 1 ELSE 0 END) AS geoNetworkmetro_LasVegasNV,
Max(CASE WHEN geoNetwork.metro='Lexington KY' THEN 1 ELSE 0 END) AS geoNetworkmetro_LexingtonKY,
Max(CASE WHEN geoNetwork.metro='Los Angeles CA' THEN 1 ELSE 0 END) AS geoNetworkmetro_LosAngelesCA,
Max(CASE WHEN geoNetwork.metro='Louisville KY' THEN 1 ELSE 0 END) AS geoNetworkmetro_LouisvilleKY,
Max(CASE WHEN geoNetwork.metro='Madison WI' THEN 1 ELSE 0 END) AS geoNetworkmetro_MadisonWI,
Max(CASE WHEN geoNetwork.metro='Mankato MN' THEN 1 ELSE 0 END) AS geoNetworkmetro_MankatoMN,
Max(CASE WHEN geoNetwork.metro='Memphis TN' THEN 1 ELSE 0 END) AS geoNetworkmetro_MemphisTN,
Max(CASE WHEN geoNetwork.metro='Miami-Ft. Lauderdale FL' THEN 1 ELSE 0 END) AS geoNetworkmetro_MiamiFtLauderdaleFL,
Max(CASE WHEN geoNetwork.metro='Milwaukee WI' THEN 1 ELSE 0 END) AS geoNetworkmetro_MilwaukeeWI,
Max(CASE WHEN geoNetwork.metro='Minneapolis-St. Paul MN' THEN 1 ELSE 0 END) AS geoNetworkmetro_MinneapolisStPaulMN,
Max(CASE WHEN geoNetwork.metro='Nashville TN' THEN 1 ELSE 0 END) AS geoNetworkmetro_NashvilleTN,
Max(CASE WHEN geoNetwork.metro='New Orleans LA' THEN 1 ELSE 0 END) AS geoNetworkmetro_NewOrleansLA,
Max(CASE WHEN geoNetwork.metro='New York NY' OR geoNetwork.metro='New York NY' THEN 1 ELSE 0 END) AS geoNetworkmetro_NewYorkNY,
Max(CASE WHEN geoNetwork.metro='Norfolk-Portsmouth-Newport News VA' THEN 1 ELSE 0 END) AS geoNetworkmetro_NorfolkPortsmouthNewportNewsVA,
Max(CASE WHEN geoNetwork.metro='Omaha NE' THEN 1 ELSE 0 END) AS geoNetworkmetro_OmahaNE,
Max(CASE WHEN geoNetwork.metro='Orlando-Daytona Beach-Melbourne FL' THEN 1 ELSE 0 END) AS geoNetworkmetro_OrlandoDaytonaBeachMelbourneFL,
Max(CASE WHEN geoNetwork.metro='Panama City FL' THEN 1 ELSE 0 END) AS geoNetworkmetro_PanamaCityFL,
Max(CASE WHEN geoNetwork.metro='Philadelphia PA' THEN 1 ELSE 0 END) AS geoNetworkmetro_PhiladelphiaPA,
Max(CASE WHEN geoNetwork.metro='Phoenix AZ' THEN 1 ELSE 0 END) AS geoNetworkmetro_PhoenixAZ,
Max(CASE WHEN geoNetwork.metro='Pittsburgh PA' THEN 1 ELSE 0 END) AS geoNetworkmetro_PittsburghPA,
Max(CASE WHEN geoNetwork.metro='Portland OR' THEN 1 ELSE 0 END) AS geoNetworkmetro_PortlandOR,
Max(CASE WHEN geoNetwork.metro='Providence-New Bedford,MA' THEN 1 ELSE 0 END) AS geoNetworkmetro_ProvidenceNewBedfordMA,
Max(CASE WHEN geoNetwork.metro='Raleigh-Durham (Fayetteville) NC' THEN 1 ELSE 0 END) AS geoNetworkmetro_RaleighDurhamFayettevilleNC,
Max(CASE WHEN geoNetwork.metro='Roanoke-Lynchburg VA' THEN 1 ELSE 0 END) AS geoNetworkmetro_RoanokeLynchburgVA,
Max(CASE WHEN geoNetwork.metro='Rochester-Mason City-Austin,IA' THEN 1 ELSE 0 END) AS geoNetworkmetro_RochesterMasonCityAustinIA,
Max(CASE WHEN geoNetwork.metro='Sacramento-Stockton-Modesto CA' THEN 1 ELSE 0 END) AS geoNetworkmetro_SacramentoStocktonModestoCA,
Max(CASE WHEN geoNetwork.metro='Salt Lake City UT' THEN 1 ELSE 0 END) AS geoNetworkmetro_SaltLakeCityUT,
Max(CASE WHEN geoNetwork.metro='San Antonio TX' THEN 1 ELSE 0 END) AS geoNetworkmetro_SanAntonioTX,
Max(CASE WHEN geoNetwork.metro='San Diego CA' THEN 1 ELSE 0 END) AS geoNetworkmetro_SanDiegoCA,
Max(CASE WHEN geoNetwork.metro='San Francisco-Oakland-San Jose CA' THEN 1 ELSE 0 END) AS geoNetworkmetro_SanFranciscoOaklandSanJoseCA,
Max(CASE WHEN geoNetwork.metro='Seattle-Tacoma WA' THEN 1 ELSE 0 END) AS geoNetworkmetro_SeattleTacomaWA,
Max(CASE WHEN geoNetwork.metro='Springfield MO' THEN 1 ELSE 0 END) AS geoNetworkmetro_SpringfieldMO,
Max(CASE WHEN geoNetwork.metro='St. Louis MO' THEN 1 ELSE 0 END) AS geoNetworkmetro_StLouisMO,
Max(CASE WHEN geoNetwork.metro='Syracuse NY' THEN 1 ELSE 0 END) AS geoNetworkmetro_SyracuseNY,
Max(CASE WHEN geoNetwork.metro='Tallahassee FL-Thomasville GA' THEN 1 ELSE 0 END) AS geoNetworkmetro_TallahasseeFLThomasvilleGA,
Max(CASE WHEN geoNetwork.metro='Tampa-St. Petersburg (Sarasota) FL' THEN 1 ELSE 0 END) AS geoNetworkmetro_TampaStPetersburgSarasotaFL,
Max(CASE WHEN geoNetwork.metro='Tri-Cities TN-VA' THEN 1 ELSE 0 END) AS geoNetworkmetro_TriCitiesTNVA,
Max(CASE WHEN geoNetwork.metro='Tucson (Sierra Vista) AZ' THEN 1 ELSE 0 END) AS geoNetworkmetro_TucsonSierraVistaAZ,
Max(CASE WHEN geoNetwork.metro='Tulsa OK' THEN 1 ELSE 0 END) AS geoNetworkmetro_TulsaOK,
Max(CASE WHEN geoNetwork.metro='Utica NY' THEN 1 ELSE 0 END) AS geoNetworkmetro_UticaNY,
Max(CASE WHEN geoNetwork.metro='Washington DC (Hagerstown MD)' THEN 1 ELSE 0 END) AS geoNetworkmetro_WashingtonDCHagerstownMD,
Max(CASE WHEN geoNetwork.metro='Weeling WV-Steubenville OH' THEN 1 ELSE 0 END) AS geoNetworkmetro_WheelingWVSteubenvilleOH,
-- Max(CASE WHEN geoNetwork.metro='not available in demo dataset' THEN 1 ELSE 0 END) AS geoNetworkmetro_notavailable, ### high corr with san francisco


FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as a,
    UNNEST(a.hits) as HITS
WHERE 
    _TABLE_SUFFIX BETWEEN session_start AND session_end
    AND geoNetwork.Country=country
GROUP BY a.fullVisitorId
)
,


# Finds the most common day based on pageviews
visitor_day AS (
SELECT 
    fullVisitorId, CASE 
    WHEN day = 1 THEN "Sunday"
    WHEN day = 2 THEN "Monday"
    WHEN day = 3 THEN "Tuesday"
    WHEN day = 4 THEN "Wednesday"
    WHEN day = 5 THEN "Thursday"
    WHEN day = 6 THEN "Friday"
    WHEN day = 7 THEN "Saturday" 
    END AS day
FROM (
        SELECT fullVisitorId, day, ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY pages_viewed DESC) AS row_num
        FROM (
                SELECT a.fullVisitorId, EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d',date)) AS day, SUM(totals.pageviews) AS pages_viewed
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as a
                    LEFT JOIN users_labeled as L
                    ON a.fullVisitorId = L.fullVisitorId
                WHERE 
                    _TABLE_SUFFIX BETWEEN session_start AND session_end
                    AND geoNetwork.Country=country
                GROUP BY fullVisitorId, day
             ) 
     )
WHERE row_num = 1 
)
,


# Finds the daypart with the most pageviews for each user; adjusts for timezones and daylight savings time
visitor_daypart AS (
SELECT fullVisitorId, daypart
FROM (
    SELECT fullVisitorId, daypart, ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY pageviews DESC) AS row_num
    FROM (
            SELECT fullVisitorId, CASE 
            WHEN hour_of_day_localized >= 1 AND hour_of_day_localized < 6 THEN 'night_1_6' 
            WHEN hour_of_day_localized >= 6 AND hour_of_day_localized < 11 THEN 'morning_6_11' 
            WHEN hour_of_day_localized >= 11 AND hour_of_day_localized < 14 THEN 'lunch_11_14' 
            WHEN hour_of_day_localized >= 14 AND hour_of_day_localized < 17 THEN 'afternoon_14_17' 
            WHEN hour_of_day_localized >= 17 AND hour_of_day_localized < 19 THEN 'dinner_17_19' 
            WHEN hour_of_day_localized >= 19 AND hour_of_day_localized < 22 THEN 'evening_19_23' 
            WHEN hour_of_day_localized >= 22 OR hour_of_day_localized = 0 THEN 'latenight_23_1'
            END AS daypart, SUM(pageviews) AS pageviews
            FROM (
                SELECT a.fullVisitorId, EXTRACT(HOUR FROM ( CASE # Add 1 hour during daylight savings (from 2017 to 2020)
                    WHEN state.dst = 1 AND date BETWEEN '20170312' AND '20171105' THEN TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL state.timezone+1 HOUR) 
                    WHEN state.dst = 1 AND date BETWEEN '20180311' AND '20181104' THEN TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL state.timezone+1 HOUR)
                    WHEN state.dst = 1 AND date BETWEEN '20190310' AND '20191103' THEN TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL state.timezone+1 HOUR)
                    WHEN state.dst = 1 AND date BETWEEN '20200308' AND '20201101' THEN TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL state.timezone+1 HOUR)
                    ELSE TIMESTAMP_ADD(TIMESTAMP_SECONDS(visitStartTime), INTERVAL state.timezone HOUR)
                    END ) ) AS hour_of_day_localized,
                    totals.pageviews AS pageviews
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as a
                LEFT JOIN (
                    SELECT states.* # Create table with state names, time zone, and whether they observe daylight savings
                    FROM UNNEST ([
                        STRUCT("Alaska" as state_name, -9 as timezone, 1 as dst),
                        STRUCT("American Samoa" as state_name, -10 as timezone, 0 as dst),
                        STRUCT("Hawaii" as state_name, -10 as timezone, 0 as dst),
                        STRUCT("California" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("Idaho" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("Nevada" as state_name, -8 as timezone, 1 as dst),
                        STRUCT("Oregon" as state_name, -8 as timezone, 1 as dst),
                        STRUCT("Washington" as state_name, -8 as timezone, 1 as dst),
                        STRUCT("Arizona" as state_name, -7 as timezone, 0 as dst),
                        STRUCT("Colorado" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("Kansas" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Montana" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("North Dakota" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Nebraska" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("New Mexico" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("South Dakota" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Texas" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Utah" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("Wyoming" as state_name, -7 as timezone, 1 as dst),
                        STRUCT("Alabama" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Arkansas" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Florida" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Iowa" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Illinois" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Indiana" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Kentucky" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Louisiana" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Michigan" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Minnesota" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Missouri" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Mississippi" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Oklahoma" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Tennessee" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Wisconsin" as state_name, -6 as timezone, 1 as dst),
                        STRUCT("Connecticut" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("District of Columbia" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Delaware" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Georgia" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Massachusetts" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Maryland" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Maine" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("North Carolina" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("New Hampshire" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("New Jersey" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("New York" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Ohio" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Pennsylvania" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Rhode Island" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("South Carolina" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Virginia" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Vermont" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("West Virginia" as state_name, -5 as timezone, 1 as dst),
                        STRUCT("Puerto Rico" as state_name, -4 as timezone, 0 as dst),
                        STRUCT("Virgin Islands" as state_name, -4 as timezone, 0 as dst)]) states) as state
                    ON a.geoNetwork.region = state.state_name
                    LEFT JOIN users_labeled as L
                    ON a.fullVisitorId = L.fullVisitorId
                    WHERE 
                        _TABLE_SUFFIX BETWEEN session_start AND session_end
                        AND geoNetwork.Country=country
            )
GROUP BY 1, 2 ) )
WHERE row_num = 1
)

# Join Label, Summary, and User Sessions Queries. The LEFT JOIN ensures that we exclude users that converted on first session
SELECT *
FROM 
  filtered_users_labeled
  LEFT JOIN summary_table USING (fullVisitorId)
  LEFT JOIN user_sessions USING (fullVisitorId)
  LEFT JOIN visitor_day USING (fullVisitorId)
  LEFT JOIN visitor_daypart USING (fullVisitorId)
WHERE labels = 0 # Remove converters for prediction query
)
,
probabilities AS (
SELECT
  fullVisitorId,
  predicted_labels,
  predicted_labels_probs
FROM
ML.PREDICT(MODEL `{my_model_L1_1}`,
(
SELECT
  * EXCEPT(labels)
FROM
  new_data))
)
# Users ranked by conversion probability, grouped by quintile
SELECT
  fullVisitorId,
  NTILE(5) OVER (ORDER BY p.prob DESC) as quintile
FROM
  probabilities, UNNEST(predicted_labels_probs) AS p
WHERE p.label = 1


