CREATE OR REPLACE FUNCTION bmsql_proc_order_status(
    IN in_w_id integer,
    IN in_d_id integer,
    INOUT in_c_id integer,
    IN in_c_last varchar(16),
    OUT out_c_first varchar(16),
    OUT out_c_middle char(2),
    OUT out_c_balance decimal(12,2),
    OUT out_o_id integer,
    OUT out_o_entry_d varchar(24),
    OUT out_o_carrier_id integer,
    OUT out_ol_supply_w_id integer[],
    OUT out_ol_i_id integer[],
    OUT out_ol_quantity integer[],
    OUT out_ol_amount decimal(12,2)[],
    OUT out_ol_delivery_d timestamp[]
) AS
$$
DECLARE
	v_order_line	record;
	v_ol_idx		integer := 1;
BEGIN
    --If C_LAST is given instead of C_ID (60%), determine the C_ID.
    IF in_c_last IS NOT NULL THEN
		in_c_id = bmsql_cid_from_clast(in_w_id, in_d_id, in_c_last);
    END IF;

    --Select the CUSTOMER
    SELECT INTO out_c_first, out_c_middle, in_c_last, out_c_balance
			c_first, c_middle, c_last, c_balance
		FROM bmsql_customer
		WHERE c_w_id=in_w_id AND c_d_id=in_d_id AND c_id = in_c_id;

    --Select the last ORDER for this customer.
    SELECT INTO out_o_id, out_o_entry_d, out_o_carrier_id
			o_id, o_entry_d, coalesce(o_carrier_id, -1)
		FROM bmsql_oorder
		WHERE o_w_id = in_w_id AND o_d_id = in_d_id AND o_c_id = in_c_id
		AND o_id = (
			SELECT max(o_id)
				FROM bmsql_oorder
				WHERE o_w_id = in_w_id AND o_d_id = in_d_id AND o_c_id = in_c_id
			);

	FOR v_order_line IN SELECT ol_i_id, ol_supply_w_id, ol_quantity,
				ol_amount, ol_delivery_d
			FROM bmsql_order_line
			WHERE ol_w_id = in_w_id AND ol_d_id = in_d_id AND ol_o_id = out_o_id
			ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number
			LOOP
	    out_ol_i_id[v_ol_idx] = v_order_line.ol_i_id;
	    out_ol_supply_w_id[v_ol_idx] = v_order_line.ol_supply_w_id;
	    out_ol_quantity[v_ol_idx] = v_order_line.ol_quantity;
	    out_ol_amount[v_ol_idx] = v_order_line.ol_amount;
	    out_ol_delivery_d[v_ol_idx] = v_order_line.ol_delivery_d;
		v_ol_idx = v_ol_idx + 1;
	END LOOP;

    WHILE v_ol_idx < 16 LOOP
		out_ol_i_id[v_ol_idx] = 0;
		out_ol_supply_w_id[v_ol_idx] = 0;
		out_ol_quantity[v_ol_idx] = 0;
		out_ol_amount[v_ol_idx] = 0.0;
		out_ol_delivery_d[v_ol_idx] = NULL;
		v_ol_idx = v_ol_idx +1;
    END LOOP;
END;
$$
Language plpgsql;