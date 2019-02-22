/*
ALL CLIENT-TPA VARIABLE FREQUENCY

Use Cases:
* Deciding which variables to use
  for a given PDW client-TPA's dataset.

For a given list of variables:
* Produce a variable-value-frequency
  table for each PDW client-TPA-dataset.
* Control how many of the highest-
  occurring values to include for
  each client-TPA-dataset-variable.
* Export the combined results to Excel.

*/

OPTIONS THREADS SASTRACE = ',,,sa' SASTRACELOC = SASLOG NOSTSUFFIX;   

/* turn off display of outputs/notes */
%MACRO ods_off;
    ODS EXCLUDE ALL;
    ODS NORESULTS;
    OPTIONS NONOTES;
%MEND;
 
/* re-enable display of outputs/notes */
%MACRO ods_on;
    ODS EXCLUDE NONE;
    ODS RESULTS;
    OPTIONS NOTES;
%MEND;

%ods_off

LIBNAME wfiles "/sasprod/users/wleone";
LIBNAME sasdata "/sasprod/ca/sasdata/Client_TPA_Availability";
LIBNAME outfiles "/sasprod/dg/shared/PDW All Client-TPA Variable Frequency/Outputs" ;

/* Choose a target core dataset */
%LET target_dataset = %SYSFUNC(LOWCASE(%SYSFUNC(CATS(&input_target_dataset.))));

/* Determine how many values should
   be included for every variable */
%LET threshold = &input_threshold.;

/* Suffix for the output file */
%LET short_user = %SUBSTR(&input_user., 1, 5) ; /* used for intermediate dataset file naming */
%LET suffix =_&short_user. ;
%MACRO null_test(user_suffix);
    %IF &user_suffix. = %THEN %RETURN;
    %ELSE %LET suffix = %SYSFUNC(CATS(&suffix., _, &user_suffix.));
%MEND; 
%null_test(&input_suffix.)

/* Tilda-separated list of variables
   for which value frequency tables
   will be created */
%LET var_list_init = %SYSFUNC(LOWCASE(%SYSFUNC(COMPRESS(&input_var_list_init.))));
/* Comma-separated list */
%LET all_var_no = %SYSFUNC(COUNTW(&var_list_init., '~ '));
%LET quoted_var_list = ;
%MACRO create_quoted_item_list;
    %DO a=1 %TO &all_var_no.;
        %IF &a. = 1
        %THEN %DO;
            %LET quoted_var_list = %QSCAN(%BQUOTE(&var_list_init.), &a., %NRQUOTE(~ ));
            %LET quoted_var_list = "%SYSFUNC(COMPRESS(&quoted_var_list., ,nk))";
        %END;
        %ELSE %DO;
            %LET iter_var = %QSCAN(%BQUOTE(&var_list_init.), &a., %NRQUOTE(~ ));
            %LET quoted_var_list = %SYSFUNC(CATX(%NRQUOTE(, )
                              , &quoted_var_list.
                              , "&iter_var."));
        %END;
    %END;
  
    %LET quoted_var_list = &quoted_var_list.;
%MEND;
%create_quoted_item_list

/* Identify the latest version of the PDW
   Data Dictionary for later use in the
   freq macro. */
%LET newest_dictionary = ;
%MACRO curr_dict;
    LIBNAME dict "/sasprod/dg/shared/PDW Data Dictionary/Code";

    /* Chose the most recently modified PDW
       data dictionary */
    %LET target_filename_substr = all_var_by_dataset;

    ODS OUTPUT members=mbr;
    PROC DATASETS
        LIBRARY=dict
        MEMTYPE=DATA
        ;
    RUN;

    PROC RANK
        DATA = mbr
        OUT = ranked_datasets_by_moddate
        DESCENDING;
        VARIABLES LastModified;
        RANKS modrank;
    RUN;

    PROC SQL NOPRINT;
                    
        SELECT
            MIN(modrank)
        INTO :min_modrank
        FROM ranked_datasets_by_moddate
        WHERE LOWCASE(name) CONTAINS "&target_filename_substr."
        ;
        
        SELECT DISTINCT
           name
        INTO :newest_dictionary
        FROM ranked_datasets_by_moddate
        WHERE CATS(modrank) = CATS(&min_modrank.)
        ;
    QUIT;

%MEND;
%curr_dict

%MACRO freq;
    DATA wfiles.all_pdw
            (RENAME=(
                PDW_Client_Name=Client_Name
                PDW_TPA_Name=TPA_Name));
        SET sasdata.all_pdw;
    RUN;
    
    /* Store # of PDW client-TPA's
       as macro variable */
    PROC SQL NOPRINT;
        SELECT CATS(COUNT(*))
        INTO :count
        FROM wfiles.all_pdw
        ;
    QUIT;

    /* Set variables to be used when
       auto-creating libnames */
    PROC SQL;
        SELECT
            client_name
            , tpa_name
            , CATS('lref', ref_id)
            , path
        INTO
            :client_1-:client_&count.
            , :tpa_1-:tpa_&count.
            , :libref_1-:libref_&count.
            , :path_1-:path_&count.
        FROM wfiles.all_pdw
        ;
    QUIT;


    /* Iterate through every client-TPA
       directory in PDW */
    %DO k=1 %TO &count.;
        %PUT (%SYSFUNC(TIME(), TIMEAMPM11.)) Loop &k./%CMPRES(&count.) (&&client_&k.. - &&tpa_&k..).;
        LIBNAME &&libref_&k.. "&&Path_&k.."
            ACCESS = Read;

        /* Only execute if the core dataset
           exists for this client-TPA */
        %IF %SYSFUNC(EXIST(&&libref_&k...&target_dataset.))
        %THEN
            %DO;
                /* If so, list variables in the target dataset */
                PROC CONTENTS DATA = &&libref_&k...&target_dataset.
                    MEMTYPE = DATA    
                    OUT = var_check
                    NOPRINT;
                RUN;

                /* Only continue if some target
                   variables exists in this dataset. */
                PROC SQL NOPRINT;
                    SELECT
                        COUNT(*)
                    INTO :query_vars
                    FROM var_check
                    WHERE name IN (&quoted_var_list.)
                    ;
                QUIT;

                %IF %SYSFUNC(CATS(&query_vars.)) = %SYSFUNC(CATS(0))
                %THEN %PUT No matches for &&client_&k.. - &&TPA_&k...;
                %ELSE
                    %DO;

                        PROC SQL NOPRINT;
                            /* count dataset records */
                            SELECT
                                COUNT(*)
                            INTO :total_records
                            FROM &&libref_&k...&target_dataset.
                            ;
                            
                            /* extract dataset name */
                            SELECT
                                CATS(name)
                            INTO :query_vars_list SEPARATED BY ", "
                            FROM var_check
                            WHERE name IN (&quoted_var_list.)
                            ;
                            
                        QUIT;
                        
                        /* Iterate over each target
                           variable that is in this dataset */
                        %DO y=1 %TO &query_vars.;
                            
                            %LET curr_var = %QSCAN(%BQUOTE(&query_vars_list.)
                                                   , &y., %NRQUOTE(, ));
                            %PUT (%SYSFUNC(TIME(), TIMEAMPM11.)) Calculating frequency table for &curr_var..;
                            
                            /* Create the frequency table for each variable */
                            PROC SQL NOPRINT;
                                CREATE TABLE outfiles.&short_user._freq_table_&y. AS
                                    SELECT
                                        "&&client_&k.." AS Client_Name
                                        , "&&tpa_&k.." AS TPA_Name
                                        , "&&path_&k.." AS Path
                                        , "&target_dataset." AS Core_Dataset
                                        , "&curr_var." AS Variable
                                        , QUOTE(CATS(&curr_var.))
                                          AS Value
                                        , COUNT(*)/&total_records.
                                            AS percent_of_records
                                        , COUNT(*)
                                            AS net_records
                                    FROM &&libref_&k...&target_dataset.
                                    GROUP BY
                                        &curr_var.
                                    ;
                            QUIT;

                            /* Prepare data for ranking */
                            PROC SORT
                                DATA = outfiles.&short_user._freq_table_&y.;
                                BY
                                    Client_Name
                                    TPA_Name
                                    Core_Dataset
                                    Variable
                                    DESCENDING net_records;
                            RUN;

                            /* Ranks variables' values by frequency */
                            PROC RANK
                                DATA = outfiles.&short_user._freq_table_&y.
                                OUT = outfiles.&short_user._rank_table_&y.;
                                BY Client_Name
                                    TPA_Name
                                    Core_Dataset
                                    Variable
                                    DESCENDING net_records;
                                VARIABLES net_records;
                                RANKS rank_var;
                            RUN;

                            /* Sort and drop lowest-frequency values
                               to compress output dataset */
                            PROC SORT
                                DATA = outfiles.&short_user._rank_table_&y.;
                                BY Client_Name
                                    TPA_Name
                                    Core_Dataset
                                    Variable
                                    rank_var;
                            RUN;
                            /* sorting by rank_var ensures highest-freq variables
                               are pushed to the top of the dataset */

                            DATA outfiles.&short_user._freq_table_&y.
                                    (DROP = rank_var);
                                SET outfiles.&short_user._rank_table_&y.;
                                IF _N_ > &threshold.
                                    THEN STOP;
                                /* Add no more than the threshold # of
                                   values by using default variable _N_
                                   on the rank_var pre-sorted dataset */
                            RUN;
                        %END;

                        /* Consolidate the variable-specific
                           datasets into one composite dataset
                           and sort the results */
                        DATA outfiles.&short_user._freq_&&libref_&k..;
                            LENGTH
                                Client_Name $50.
                                TPA_Name $50.
                                Path $50.
                                Core_Dataset $50.
                                Variable $100.
                                Value $500.
                                ;
                            FORMAT 
                                Client_Name $50.
                                TPA_Name $50.
                                Path $50.
                                Core_Dataset $50.
                                Variable $100.
                                Value $500.
                                percent_of_records 6.2
                                net_records 12.
                                ;
                            SET outfiles.&short_user._freq_table:;
                        RUN;

                        /* Delete intermediate datasets */
                        PROC DATASETS LIBRARY=outfiles MEMTYPE=DATA NOLIST;
                            DELETE
                                &short_user._freq_table:
                                &short_user._rank_table:
                                var_check
                            ;
                        RUN;

                    %END;
            %END;
        
        %ELSE %PUT &target_dataset. does not exist for &&client_&k.. - &&tpa_&k...;
    %END;
    
    /* Combine all client-TPA specific datasets */
    %LET today = %SYSFUNC(TODAY(), DATE9.);
    %LET counter = 1;
    %DO %UNTIL(%SYSFUNC(CATS(&counter.))
            = %SYSFUNC(CATS(&count.)));
        %LET counter = %EVAL(%SYSFUNC(SUM(&counter., 1)));
        %IF %SYSFUNC(EXIST(outfiles.&short_user._freq_&&libref_&counter..))
            %THEN
                %DO;
                    
                    %LET currdict = dict.%SYSFUNC(CATS(&newest_dictionary.));
                    %LET currvfr = outfiles.vfr_&today.&suffix.;
                    
                    DATA &currvfr.;
                        SET outfiles.&short_user._freq:;
                    RUN;

                    /* Create output file names using inputs */
                    %LET output = %SYSFUNC(TRANWRD(&currvfr., vfr_, fvfr_));
                    %LET doutput = %SYSFUNC(TRANWRD(&currvfr., vfr_, fdict_));
                    %LET outputxlsx = %SYSFUNC(TRANWRD(&currvfr., %NRQUOTE(outfiles.vfr_), final_));

                    PROC SQL NOPRINT;
                        /* Combine dictionary and VFR data */
                        CREATE TABLE &output. AS
                            SELECT
                                v.client_name
                                , v.tpa_name
                                , v.path
                                , v.variable
                                , d.type
                                , d.length
                                , d.non_null
                                , v.value
                                , v.percent_of_records
                                , v.net_records
                            FROM &currvfr. AS v
                            LEFT JOIN &currdict. AS d
                                ON v.path = d.path
                                    AND v.variable = d.variable
                                    AND LOWCASE(d.core_dataset) = "&input_target_dataset."
                            ;

                        CREATE TABLE &doutput. AS
                            SELECT DISTINCT
                                client_name
                                , tpa_name
                                , path
                                , variable
                                , type
                                , length
                                , non_null
                            FROM &output.
                            ;
                    QUIT;

                    PROC SORT
                        DATA = &output.;
                        BY
                            variable
                            client_name
                            tpa_name
                            DESCENDING percent_of_records
                            ;
                    RUN;

                    PROC SORT
                        DATA = &doutput.;
                        BY
                            variable
                            client_name
                            tpa_name
                            ;
                    RUN;

                    /* Create a two-sheet output Excel workbook */
                    PROC EXPORT
                        DATA = &output.
                        OUTFILE= "/sasprod/dg/shared/PDW All Client-TPA Variable Frequency/Outputs/&outputxlsx..xlsx"
                        DBMS = xlsx
                        REPLACE
                        ;
                        SHEET='Value Level'
                        ;
                    RUN;

                    PROC EXPORT
                        DATA = &doutput.
                        OUTFILE= "/sasprod/dg/shared/PDW All Client-TPA Variable Frequency/Outputs/&outputxlsx..xlsx"
                        DBMS = xlsx
                        REPLACE
                        ;
                        SHEET='Variable Level'
                        ;
                    RUN;

                     %IF %SYSFUNC(EXIST(outfiles.vfr_&today.&suffix.))
                        %THEN
                            %DO;
                                /* Delete intermediate datasets */
                                PROC DATASETS LIBRARY=outfiles MEMTYPE=DATA NOLIST;
                                    DELETE
                                        &short_user._freq:
                                    ;
                                RUN;
                            %END;
                    %ELSE %PUT Combined dataset creation failed. Retaining intermediate datasets.;

                    %RETURN;
                %END;
        %ELSE
            %DO;
                %IF %SYSFUNC(CATS(&counter.))
                        = %SYSFUNC(CATS(&count.))
                    %THEN
                        %DO;
                            %PUT WARNING: Your query yielded no results.;
                            %RETURN;
                        %END;
                    %ELSE ;
            %END;
    %END;

%MEND;
%freq
