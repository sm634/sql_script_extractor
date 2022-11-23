/*
############################################################################### last_far_outcome
##### Filters outcomes to last admin review decision
*/
INSERT INTO [hybrid_layer_dev].[derived].last_far_outcome  WITH (TABLOCK) (
	[far_outcome],	
	[far_outcome_code],
	[far_outcome_date],
	[far_outcome_unit_code],
	[far_outcome_unit],
	[far_outcome_by],
	[far_outcome_case_id],
	[far_outcome_coh_id],
	[far_outcome_scat],
	[far_outcome_no]
)

SELECT
	admin_review_outcomes.case_outcome											-- far_outcome	
  , admin_review_outcomes.case_outcome_code								-- far_outcome_code
  , admin_review_outcomes.outcome_date										-- far_outcome_date
  , admin_review_outcomes.case_outcome_unit_code					-- far_outcome_unit_code
  , admin_review_outcomes.case_outcome_unit								-- far_outcome_unit
  , admin_review_outcomes.case_outcome_by									-- far_outcome_by
  , admin_review_outcomes.cas_id													-- far_outcome_case_id
  , admin_review_outcomes.coh_id													-- far_outcomet_coh_id
	, admin_review_outcomes.stats_cat_name
  , admin_review_outcomes.outcome_no											-- far_outcome_no

FROM 
  hybrid_layer_dev.derived.admin_review_outcomes

  INNER JOIN (
      SELECT
        admin_review_outcomes.cas_id
        , MAX(admin_review_outcomes.outcome_no)									AS far_decision_no
      FROM
        hybrid_layer_dev.derived.admin_review_outcomes
        INNER JOIN hybrid_layer_dev.derived.admin_review_first_ar_outcome
          ON admin_review_outcomes.cas_id = admin_review_first_ar_outcome.ar_case_id
          AND admin_review_outcomes.outcome_no > admin_review_first_ar_outcome.ar_first_outcome_no
      WHERE
        admin_review_outcomes.case_outcome_code IN ('ARDM', 'ARDO', 'ARR','ARW')
      GROUP BY
         admin_review_outcomes.cas_id
    )																														AS far_decision
    ON admin_review_outcomes.cas_id = far_decision.cas_id
      AND admin_review_outcomes.outcome_no = far_decision.far_decision_no
  ;