SELECT company,  company NOT rlike 'Group', company REGEXP 'Group', company SIMILAR TO 'Group', company REGEXP '['  FROM tests.data.customers;
