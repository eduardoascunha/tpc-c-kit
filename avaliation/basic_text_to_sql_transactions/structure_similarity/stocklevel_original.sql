CREATE OR REPLACE FUNCTION bmsql_proc_stock_level(
    IN in_w_id integer,
    IN in_d_id integer,
    IN in_threshold integer,
    OUT out_low_stock integer
) AS
$$
BEGIN
    SELECT INTO out_low_stock
			count(*) AS low_stock
		FROM (
			SELECT s_w_id, s_i_id, s_quantity
			FROM bmsql_stock
			WHERE s_w_id = in_w_id AND s_quantity < in_threshold
			  AND s_i_id IN (
				SELECT ol_i_id
					FROM bmsql_district
					JOIN bmsql_order_line ON ol_w_id = d_w_id
					 AND ol_d_id = d_id
					 AND ol_o_id >= d_next_o_id - 20
					 AND ol_o_id < d_next_o_id
					WHERE d_w_id = in_w_id AND d_id = in_d_id
				)
			) AS L;
END;
$$
LANGUAGE plpgsql;