(CASE LENGTH(to_char(NUMSEQUENCIAL))
            WHEN 1
                THEN '000000' || to_char(NUMSEQUENCIAL) 
                   || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                        END) 
                WHEN 2
                   THEN '00000' || to_char(NUMSEQUENCIAL) 
                   || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                        END) 
               WHEN 3
                   THEN '0000' || to_char(NUMSEQUENCIAL) 
                   || (
                       case length(to_char(DV)) 
                        when 1 then '0' || to_char(dv)
                        when 2 then to_char(dv)
                        else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                        END) 
               WHEN 4
               THEN '000' || to_char(NUMSEQUENCIAL) 
               || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                   || 
                  (CASE LENGTH(to_char(codlocregi))
                       WHEN 0
                       THEN '0000'  || to_char(CODLOCREGI)
                       WHEN 1
                        THEN '000'  || to_char(CODLOCREGI)
                       WHEN 2
                        THEN '00'  || to_char(CODLOCREGI)
                       WHEN 3
                        THEN '0'  || to_char(CODLOCREGI)
                       ELSE to_char(codlocregi)
                    END)
                WHEN 5
              THEN '00' || to_char(NUMSEQUENCIAL) 
                || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                   || 
                   (CASE LENGTH(to_char(codlocregi))
                       WHEN 0
                       THEN '0000'  || to_char(CODLOCREGI)
                       WHEN 1
                        THEN '000'  || to_char(CODLOCREGI)
                       WHEN 2
                        THEN '00'  || to_char(CODLOCREGI)
                       WHEN 3
                        THEN '0'  || to_char(CODLOCREGI)
                       ELSE to_char(codlocregi)
                    END)
                WHEN 6
                THEN '0' || to_char(NUMSEQUENCIAL) 
                    || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                   || 
                   (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                    END)
                    WHEN 7
                    THEN to_char(NUMSEQUENCIAL) 
                        || (
                       case length(to_char(DV)) 
                        when 1 then '0' || to_char(dv)
                        when 2 then to_char(dv)
                        else null end)
                            to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                               WHEN 0
                               THEN '0000'  || to_char(CODLOCREGI)
                               WHEN 1
                                THEN '000'  || to_char(CODLOCREGI)
                               WHEN 2
                                THEN '00'  || to_char(CODLOCREGI)
                               WHEN 3
                                THEN '0'  || to_char(CODLOCREGI)
                               ELSE to_char(codlocregi)
                        END)
                ELSE to_char(NUMSEQUENCIAL) END)