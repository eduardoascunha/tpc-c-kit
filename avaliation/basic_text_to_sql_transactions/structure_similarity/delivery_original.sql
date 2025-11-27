CREATE OR REPLACE FUNCTION bmsql_proc_delivery_bg(
	IN in_w_id integer,
	IN in_o_carrier_id integer,
	IN in_ol_delivery_d timestamp,
	OUT out_delivered_o_id integer[]
) AS
$$
DECLARE
	var_d_id integer;
	var_o_id integer;
	var_c_id integer;
	var_sum_ol_amount decimal(12, 2);
BEGIN
	FOR var_d_id IN 1..10 LOOP
		var_o_id = -1;
		/*
		 * Try to find the oldest undelivered order for this
		 * DISTRICT. There may not be one, which is a case
		 * that needs to be reported.
		*/
		WHILE var_o_id < 0 LOOP
			SELECT INTO var_o_id
					no_o_id
				FROM bmsql_new_order
			WHERE no_w_id = in_w_id AND no_d_id = var_d_id
			ORDER BY no_o_id ASC;
			IF NOT FOUND THEN
			    var_o_id = -1;
				EXIT;
			END IF;

			DELETE FROM bmsql_new_order
				WHERE no_w_id = in_w_id AND no_d_id = var_d_id
				  AND no_o_id = var_o_id;
			IF NOT FOUND THEN
			    var_o_id = -1;
			END IF;
		END LOOP;

		IF var_o_id < 0 THEN
			-- No undelivered NEW_ORDER found for this District.
			var_d_id = var_d_id + 1;
			CONTINUE;
		END IF;

		/*
		 * We found out oldert undelivered order for this DISTRICT
		 * and the NEW_ORDER line has been deleted. Process the
		 * rest of the DELIVERY_BG.
		*/

		-- Update the ORDER setting the o_carrier_id.
		UPDATE bmsql_oorder
			SET o_carrier_id = in_o_carrier_id
			WHERE o_w_id = in_w_id AND o_d_id = var_d_id AND o_id = var_o_id;

		-- Get the o_c_id from the ORDER.
		SELECT INTO var_c_id
				o_c_id
			FROM bmsql_oorder
			WHERE o_w_id = in_w_id AND o_d_id = var_d_id AND o_id = var_o_id;

		-- Update ORDER_LINE setting the ol_delivery_d.
		UPDATE bmsql_order_line
			SET ol_delivery_d = in_ol_delivery_d
			WHERE ol_w_id = in_w_id AND ol_d_id = var_d_id
			  AND ol_o_id = var_o_id;

		-- SELECT the sum(ol_amount) from ORDER_LINE.
		SELECT INTO var_sum_ol_amount
				sum(ol_amount) AS sum_ol_amount
			FROM bmsql_order_line
			WHERE ol_w_id = in_w_id AND ol_d_id = var_d_id
			  AND ol_o_id = var_o_id;

		-- Update the CUSTOMER.
		UPDATE bmsql_customer
			SET c_balance = c_balance + var_sum_ol_amount,
				c_delivery_cnt = c_delivery_cnt + 1
			WHERE c_w_id = in_w_id AND c_d_id = var_d_id and c_id = var_c_id;

		out_delivered_o_id[var_d_id] = var_o_id;

		var_d_id = var_d_id +1 ;
	END LOOP;
END;
$$
LANGUAGE plpgsql;