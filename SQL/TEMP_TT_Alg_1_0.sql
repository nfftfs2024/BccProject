IF OBJECT_ID('TT_Alg_1_0') IS NOT NULL DROP TABLE TT_Alg_1_0	-- Replace existing data in table
SELECT 
	V.*
	, CASE
		WHEN V.SUBT_DC BETWEEN 2 AND 120 THEN 1		-- Flag data rows with traveling time between 2 to 120 mins with 1
		ELSE 0 										-- Otherwise flag with 0
	END TT_Alg_Flag
INTO TT_Alg_1_0
FROM (
	SELECT 
		W.*,
		LEAD(W.ENTERED_TIME,1,NULL) OVER (ORDER BY W.DEVICEID, W.ENTERED_TIME) LEADTIME,	-- Get ENTERED_TIME of next row
		datediff(second, W.ENTERED_TIME, LEAD(W.ENTERED_TIME,1,NULL) OVER (ORDER BY W.DEVICEID, W.ENTERED_TIME)) / 60 SUBT_DC	-- Calculate the time difference between ENTERED_TIME and ENTERED_TIME of the next row in minutes
	FROM (
		SELECT
			X.*
		FROM (
			SELECT
				Y.*
				, Z.COUNT, 
				LEAD(Y.DEVICEID,1,Y.DEVICEID) OVER (ORDER BY Y.DEVICEID, Y.ENTERED_TIME) LEADDEV, 	-- Get DEVICEID of next row
				LEAD(Y.AREAID,1,Y.AREAID) OVER (ORDER BY Y.DEVICEID, Y.ENTERED_TIME) LEADAREA,		-- Get AREAID of next row
				LAG(Y.DEVICEID,1,Y.DEVICEID) OVER (ORDER BY Y.DEVICEID, Y.ENTERED_TIME) LAGDEV,		-- Get DEVICEID of previous row
				LAG(Y.AREAID,1,Y.AREAID) OVER (ORDER BY Y.DEVICEID, Y.ENTERED_TIME) LAGAREA			-- Get AREAID of previous row
			FROM BLUETOOTH_DETAILS Y
			LEFT OUTER JOIN (
				SELECT 
					Z1.DEVICEID
					, Z1.TIME
					, COUNT(Z1.DEVICEID) [COUNT]
				FROM (
					SELECT DISTINCT 
						DEVICEID
						, AREAID
						, CONVERT(VARCHAR(10), ENTERED_TIME, 112) TIME	-- DISTINCT data by DEVICEID, AREAID and detected date
					FROM BLUETOOTH_DETAILS ) Z1
					GROUP BY Z1.DEVICEID, Z1.TIME 	-- Group by DEVICEID and detected date
			) Z ON (
				Y.DEVICEID = Z. DEVICEID
				AND CONVERT(VARCHAR(10), Y.ENTERED_TIME, 112) = Z.TIME
			)
			WHERE Z.COUNT > 1		-- Filtering out data rows with only 1 record
		) X
		WHERE (
			X.AREAID != X.LEADAREA		
			AND X.DEVICEID = X.LEADDEV	 
		) OR (
			X.AREAID != X.LAGAREA		 
			AND X.DEVICEID = X.LAGDEV	 
		)	-- Filtering out data rows having same DEVICEID but different AREAID in its next row, or
			-- data rows having same DEVICEID but different AREAID in its previous row
	) W
) V
WHERE 
	V.DEVICEID = V.LEADDEV				-- Checking and filtering out next data row having different DEVICEID
	AND V.AREAID != V.LEADAREA			-- Checking and filtering out next data row having the same AREAID
ORDER BY V.DEVICEID, V.ENTERED_TIME		-- Order data by DEVICEID and ENTERED_TIME