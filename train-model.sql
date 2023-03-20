
#Create Model
/*
This query creates a propensity model using logistic regression.
*/

/*
Classification Models:
- LOGISTIC_REG
- BOOSTED_TREE_CLASSIFIER
- RANDOM_FOREST_CLASSIFIER
- DNN_CLASSIFIER
- DNN_LINEAR_COMBINED_CLASSIFIER
*/



-- Train a logistic regression using BQML.
CREATE OR REPLACE MODEL `{model_name}`
OPTIONS(
  MODEL_TYPE ='logistic_reg'
, INPUT_LABEL_COLS = ['labels']
, L1_REG = 1
, DATA_SPLIT_METHOD = 'RANDOM'
, DATA_SPLIT_EVAL_FRACTION = 0.20
) AS
SELECT * EXCEPT(fullVisitorId, first_conversion_session, last_conversion_timestamp, last_visit_timestamp)
FROM `{training_data_table}`
;


# Review Training Info
SELECT * FROM ML.TRAINING_INFO(MODEL {model_name});
# Review Model Weights
SELECT * FROM ML.WEIGHTS(MODEL {model_name}, STRUCT(TRUE
AS standardize)) ORDER by weight DESC;
# Confusion Matrix
SELECT * FROM ML.CONFUSION_MATRIX(MODEL {model_name});
# ROC CURVE
SELECT * FROM ML.ROC_CURVE(MODEL {model_name});
# Evaluate Model
SELECT * FROM ML.EVALUATE(MODEL {model_name});



