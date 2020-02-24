DROP PROCEDURE IF EXISTS fill_date_dimension; ---

CREATE PROCEDURE fill_date_dimension(IN startdate DATE,IN stopdate DATE)
BEGIN
	DECLARE curdate DATE;
	SET curdate = startdate;
	WHILE curdate < stopdate DO
		INSERT INTO D_TIME (DATE, YEAR, MONTH, MONTH_NAME, MONTH_NOMBRE,
				DAY, DAY_NAME, DAY_NOMBRE, WEEK, WEEKEND_FLAG)
			VALUES (
			curdate,
			YEAR(curdate),
			MONTH(curdate),
			DATE_FORMAT(curdate, '%M'),
			ELT(MONTH(curdate), 'enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre', 'octubre', 'noviembre','diciembre'),
                        DAY(curdate),
                        DATE_FORMAT(curdate, '%W'),
			ELT(WEEKDAY(curdate)+1, 'Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo'),
			WEEKOFYEAR(curdate),
			CASE DAYOFWEEK(curdate) WHEN 1 THEN 1 WHEN 7 then 1 ELSE 0 END
			);
		SET curdate = ADDDATE(curdate, INTERVAL 1 DAY);
	END WHILE;
END
---

SET FOREIGN_KEY_CHECKS = 0; ---

TRUNCATE TABLE D_TIME; ---

SET FOREIGN_KEY_CHECKS = 1;---

CALL fill_date_dimension('2010-01-01', '2100-01-01'); ---

OPTIMIZE TABLE D_TIME; ---

