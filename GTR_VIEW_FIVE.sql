
SELECT 
NB
,BUYER_IDENTIFIER
--,PAYER_IDENTIFIER_LEG_1 
FROM GTR_VIEW_FIVE
ORDER BY TO_NUMBER(NB)

CREATE OR REPLACE FORCE EDITIONABLE VIEW "TMF_SERVICES"."GTR_VIEW_FIVE" AS 
-- SWITCH TO GTR_NB_LIST
-- 
WITH EODDATE AS (
    SELECT DATE '2026-03-17' AS MAX_VD FROM DUAL    
),

GTR_BASE_TRADES AS (
    SELECT
        T.NB
        ,T.THD_VALUE_DATE
        ,T.SYS_PARTITION_ID
        ,T.HS_CNTRP
        ,T.D_CTP_ENT
        ,T.H_INT
        ,T.TP_STATUS2
        ,T.H_PRODUCT
        ,T.TRN_FMLY
        ,T.TRN_GRP
        ,T.TRN_TYPE
        ,T.M_EVT_DATE
        ,T.M_CFI
        ,T.H_DVCS
        ,T.C_CUR_PL
        ,T.TP_RTCUR0
        ,T.M_TP_MSTRUCT
        ,T.M_TP_CRISDA
        ,T.M_BASKET_IND
        ,T.TP_QTYEQ
        ,T.H_PUTCALL
        ,T.TP_IQTY
        ,T.TP_FXSTNUM
        ,T.TP_FXSTDEN
        ,T.M_PKG_CURR_R
        ,T.M_PKG_TYP_R
        ,T.H_PKG_LOG_ID
        ,T.PACKAGE
        ,T.CNT_TYPO
        ,T.PKG_CLASS
        ,T.M_BUSINE_EVT        
        ,T.M_PKG_VAL_R        
        ,T.M_PKG_NOTA_R        
        ,T.CONTRACT_NUMBER
        ,T.HS_UPI        
        ,T.H_TP_AE
        ,T.TP_OBAR1
        ,T.HS_EXEC_TIME
        ,T.M_COMPRES_ID
        ,T.M_VAL_METHOD
        ,T.H_RTDPP01
        ,T.TP_RTCCP02        
        ,T.M_HS_PCK_DSC    
        ,T.U_OPTTYPE
        ,T.M_CLEAR_STS
        ,T.M_AUTO_CLR
        ,T.M_CCP_NAME
        ,T.HS_REPROLCFTC
        ,T.HS_REPROLCSA
        ,T.HS_MUTUALP
        ,T.TP_CANCBY
        ,T.M_SCHEDULE
        ,T.H_CNTRPCY
        ,T.M_MARGIN_IND
        ,T.HS_MAT_LAB
        ,T.H_DTETRN
        ,T.H_DTELST
        ,T.H_UNADJMAT
        ,T.H_UNADJST0
        ,T.H_BS
        ,T.TP_RTNBLEG
        ,T.DEAL_LEG
        ,T.H_IPAY
        ,T.TP_LENTDSP
        ,T.TP_CRDPROT
             
    FROM TB_TRADE_MUREX_AND_TB_SMARTCO_COUNTERPARTY T
--    FROM TB_TRADE_MUREX_GTR_A T
    --WHERE T.NB IN (SELECT GTR_NB FROM GTR_ID_LIST)
),

TRD_CANDIDATE AS (
    SELECT
        TRD.*
    FROM GTR_BASE_TRADES TRD
),

RELEVANT_CPTY_KEYS AS (
    SELECT DISTINCT
        THD_VALUE_DATE AS RPY_VALUE_DATE,
        HS_CNTRP       AS MUREX_COUNTERPART
    FROM TRD_CANDIDATE
    WHERE HS_CNTRP IS NOT NULL

    UNION

    SELECT DISTINCT
        THD_VALUE_DATE AS RPY_VALUE_DATE,
        D_CTP_ENT      AS MUREX_COUNTERPART
    FROM TRD_CANDIDATE
    WHERE H_INT = 'Y'
      AND D_CTP_ENT IS NOT NULL
),

CPTY_FILTERED AS (
    SELECT
        C.MUREX_COUNTERPART
        ,C.RPY_VALUE_DATE
        ,C.AGENT_ID
        ,C.NAME
        ,C.LEI
        ,C.CPTY_ID_SOURCE_PRIV_LAW_IDENT
        ,C.CPTY_ID_SOURCE_NAT_PERS_IND
        ,C.FINANCIAL_ENTITY
        ,C.CLR_EX_END_USER_EXCEPTION
        ,C.CLR_EX_SMALL_BK_EXEMPTION
        ,C.CLR_EX_OTHER_EXCEPTIONS
        ,C.CLR_EX_NO_ACT_LETTER
        ,C.CLR_EX_COOP_EXEMPTION
        ,C.SYS_CREAT_DTMS
        ,C.FED_ENTITY_INDICATOR
        ,C.CLEARING_THRESHOLD_EXEEDED    
        ,C.CANADIAN_DERIVATIVE_STATUS
        ,C.CANADA
        
    FROM SDH.TB_SMARTCO_COUNTERPARTY C
    INNER JOIN RELEVANT_CPTY_KEYS K
        ON K.MUREX_COUNTERPART = C.MUREX_COUNTERPART
       AND K.RPY_VALUE_DATE    = C.RPY_VALUE_DATE
),

CPTY_DEDUP AS (
    SELECT
        MUREX_COUNTERPART
        ,RPY_VALUE_DATE
        ,AGENT_ID
        ,NAME
        ,LEI
        ,CPTY_ID_SOURCE_PRIV_LAW_IDENT
        ,CPTY_ID_SOURCE_NAT_PERS_IND
        ,FINANCIAL_ENTITY
        ,CLR_EX_END_USER_EXCEPTION
        ,CLR_EX_SMALL_BK_EXEMPTION
        ,CLR_EX_OTHER_EXCEPTIONS
        ,CLR_EX_NO_ACT_LETTER
        ,CLR_EX_COOP_EXEMPTION
        ,CLEARING_THRESHOLD_EXEEDED    
        ,CANADIAN_DERIVATIVE_STATUS
        ,CANADA
        
        ,FED_ENTITY_INDICATOR
        ,ROW_NUMBER() OVER (
            PARTITION BY MUREX_COUNTERPART, RPY_VALUE_DATE
            ORDER BY SYS_CREAT_DTMS DESC NULLS LAST
        ) AS RN
    
    FROM CPTY_FILTERED
),

MATCHED_CPTY AS (
    SELECT
        TRD.*
 
        ,COALESCE(CPTY_HS.AGENT_ID, CPTY_DCTP.AGENT_ID) AS AGENT_ID
        ,COALESCE(CPTY_HS.NAME, CPTY_DCTP.NAME) AS CPTY_NAME
        ,COALESCE(CPTY_HS.LEI, CPTY_DCTP.LEI) AS CPTY_LEI
        ,COALESCE(CPTY_HS.CPTY_ID_SOURCE_PRIV_LAW_IDENT, CPTY_DCTP.CPTY_ID_SOURCE_PRIV_LAW_IDENT) AS CPTY_ID_SOURCE_PRIV_LAW_IDENT
        ,COALESCE(CPTY_HS.CPTY_ID_SOURCE_NAT_PERS_IND, CPTY_DCTP.CPTY_ID_SOURCE_NAT_PERS_IND) AS CPTY_ID_SOURCE_NAT_PERS_IND
        ,COALESCE(CPTY_HS.FINANCIAL_ENTITY, CPTY_DCTP.FINANCIAL_ENTITY) AS FINANCIAL_ENTITY
        ,COALESCE(CPTY_HS.CLR_EX_END_USER_EXCEPTION, CPTY_DCTP.CLR_EX_END_USER_EXCEPTION) AS CLR_EX_END_USER_EXCEPTION
        ,COALESCE(CPTY_HS.CLR_EX_SMALL_BK_EXEMPTION, CPTY_DCTP.CLR_EX_SMALL_BK_EXEMPTION) AS CLR_EX_SMALL_BK_EXEMPTION
        ,COALESCE(CPTY_HS.CLR_EX_COOP_EXEMPTION, CPTY_DCTP.CLR_EX_COOP_EXEMPTION) AS CLR_EX_COOP_EXEMPTION
        ,COALESCE(CPTY_HS.CLR_EX_OTHER_EXCEPTIONS, CPTY_DCTP.CLR_EX_OTHER_EXCEPTIONS) AS CLR_EX_OTHER_EXCEPTIONS
        ,COALESCE(CPTY_HS.CLR_EX_NO_ACT_LETTER, CPTY_DCTP.CLR_EX_NO_ACT_LETTER) AS CLR_EX_NO_ACT_LETTER
        ,COALESCE(CPTY_HS.FED_ENTITY_INDICATOR, CPTY_DCTP.FED_ENTITY_INDICATOR) AS FED_ENTITY_INDICATOR

        ,COALESCE(CPTY_HS.CLEARING_THRESHOLD_EXEEDED, CPTY_DCTP.CLEARING_THRESHOLD_EXEEDED) AS CLEARING_THRESHOLD_EXEEDED
        ,COALESCE(CPTY_HS.CANADIAN_DERIVATIVE_STATUS, CPTY_DCTP.CANADIAN_DERIVATIVE_STATUS) AS CANADIAN_DERIVATIVE_STATUS
        ,COALESCE(CPTY_HS.CANADA, CPTY_DCTP.CANADA) AS CANADA
        
        ,CASE
            WHEN CPTY_HS.AGENT_ID IS NOT NULL THEN 1
            WHEN CPTY_DCTP.AGENT_ID IS NOT NULL THEN 2
            ELSE NULL
         END AS MATCH_PRIORITY
 
        ,CASE
            WHEN CPTY_HS.AGENT_ID IS NOT NULL THEN 'HS_CNTRP'
            WHEN CPTY_DCTP.AGENT_ID IS NOT NULL THEN 'D_CTP_ENT'
            ELSE NULL
         END AS MATCH_SOURCE
 
    FROM TRD_CANDIDATE TRD
 
    LEFT JOIN CPTY_DEDUP CPTY_HS
      ON CPTY_HS.RN = 1
     AND CPTY_HS.RPY_VALUE_DATE = TRD.THD_VALUE_DATE
     AND CPTY_HS.MUREX_COUNTERPART = TRD.HS_CNTRP
 
    LEFT JOIN CPTY_DEDUP CPTY_DCTP
      ON CPTY_DCTP.RN = 1
     AND TRD.H_INT = 'Y'
     AND CPTY_HS.AGENT_ID IS NULL
     AND CPTY_DCTP.RPY_VALUE_DATE = TRD.THD_VALUE_DATE
     AND CPTY_DCTP.MUREX_COUNTERPART = TRD.D_CTP_ENT
),

DELIVERY_EVAL AS (
    SELECT
        A.*,
        CASE
            WHEN NULLIF(TRIM(A.M_CFI), '') IS NOT NULL
                 AND SUBSTR(TRIM(A.M_CFI), -1) = 'C' THEN 'CASH'
            WHEN NULLIF(TRIM(A.M_CFI), '') IS NOT NULL
                 AND SUBSTR(TRIM(A.M_CFI), -1) = 'P' THEN 'PHYS'
            WHEN NULLIF(TRIM(A.M_CFI), '') IS NOT NULL
                 AND SUBSTR(TRIM(A.M_CFI), -1) = 'E' THEN 'OPTL'
            WHEN A.TRN_FMLY = 'CURR' THEN
                DECODE(A.H_DVCS, 'C', 'CASH', 'D', 'PHYS', NULL)
            WHEN A.TRN_FMLY = 'IRD' THEN
                CASE
                    WHEN A.H_PRODUCT = 'IRD_OSWP'
                        THEN DECODE(A.H_DVCS, 'D', 'PHYS', 'C', 'CASH', NULL)
                    WHEN A.H_PRODUCT NOT IN ('IRD_FRA', 'IRD_TRS')
                         AND NVL(A.C_CUR_PL, ' ') <> NVL(A.TP_RTCUR0, ' ')
                        THEN 'CASH'
                    ELSE 'PHYS'
                END
            WHEN A.TRN_FMLY = 'EQD' OR A.H_PRODUCT = 'IRD_RTRN_BSK' THEN
                CASE
                    WHEN A.TRN_GRP IN ('EQUIT', 'OPT')
                        THEN DECODE(A.H_DVCS, 'C', 'CASH', 'D', 'PHYS', 'Z', 'OPTL', NULL)
                    ELSE NULL
                END
            WHEN A.TRN_FMLY = 'CRD' THEN
                CASE
                    WHEN A.TRN_GRP IN ('RTRN', 'TRS') THEN
                        CASE
                            WHEN A.M_TP_MSTRUCT LIKE '%BRS%' THEN NULL
                            ELSE 'CASH'
                        END
                    ELSE
                        DECODE(
                            A.M_TP_CRISDA,
                            'Cash',             'CASH',
                            'Delivery',         'PHYS',
                            'Cash or delivery', 'OPTL',
                            'Auction',          'OPTL',
                            NULL
                        )
                END
            ELSE NULL
        END AS DELIVERY_TYPE
    FROM MATCHED_CPTY A --RANKED_CPTY A
)

,NOTIONALS AS (
    SELECT
        FFM.NB,
        LISTAGG(TO_CHAR(FFM.DT_CAPREM0), ','
            ON OVERFLOW TRUNCATE '...' WITHOUT COUNT
        ) WITHIN GROUP (ORDER BY FFM.LEGSTEP) AS CONCAT_CAPREM0
    FROM    SDH.TB_FIXFLOW_MUREX FFM
    INNER JOIN GTR_BASE_TRADES GBT
        ON  GBT.NB = FFM.NB
       AND  FFM.FFL_VALUE_DATE = GBT.THD_VALUE_DATE
    WHERE   
        FIXVAR      = 'F1'
        AND   FIXVAR1     = 'F' 
        AND   AMORTFLAG   = 'Y'
        AND   EQDSIDE     = 'N'
        AND FFM.DATE4 <= ADD_MONTHS(FFM.FFL_VALUE_DATE, 60)
        --AND DATE1       IS NOT NULL
        --AND   DT_LGPR0    = 'P'

    GROUP BY FFM.NB
)
,PACKAGE_INDICATOR AS (
    SELECT
        DE.*
        ,N.CONCAT_CAPREM0
        ,CASE
            WHEN H_PRODUCT = 'CURR_FXD_SWLEG' THEN 'TRUE'
            WHEN H_PRODUCT = 'CURR_FXD_FXD'
                 AND M_BUSINE_EVT = 'Exercise'
            THEN 'FALSE'
            WHEN PKG_CLASS = 'internal' THEN 'FALSE'
            WHEN PKG_CLASS = 'single'
                 AND TRIM(H_PKG_LOG_ID) IS NULL
            THEN 'FALSE'
            WHEN CNT_TYPO IN ('Bespoke CDS', 'INVSMT ALLOC METHD')
            THEN 'FALSE'
            WHEN NVL(PACKAGE, 0) <> 0
            THEN 'TRUE'
            WHEN TRIM(H_PKG_LOG_ID) IS NOT NULL
            THEN 'TRUE'
            ELSE 'FALSE'
        END AS PACKAGE_INDICATOR_RAW
    FROM DELIVERY_EVAL DE
    LEFT JOIN NOTIONALS N
        ON N.NB = DE.NB
)

,TRADE_FLAGS AS (
    SELECT
        PI.*
        /* CLEARED - Override existing logic */
        ,CASE
            WHEN PI.M_CCP_NAME IS NULL THEN 'N'
            WHEN UPPER(TRIM(NVL(PI.M_AUTO_CLR, ' '))) IN ('Y','TRUE') THEN 'I'
            WHEN UPPER(TRIM(NVL(PI.M_CLEAR_STS, ' '))) = 'REGISTERED' THEN 'Y'
            ELSE 'N'
        END AS CLEARED
        
        /* CAN_CLEARABLE */
        ,CASE
            WHEN HS_UPI = 'InterestRate:IRSwap:OIS'
                --AND HS_CNTRP NOT LIKE '%CSA%'
                AND M_SCHEDULE = 'C'
                AND MONTHS_BETWEEN(
                        NVL(H_UNADJMAT, H_DTELST),
                        NVL(H_UNADJST0, H_DTETRN)
                    ) / 12 <= 10
                
                --AND TO_NUMBER(REPLACE(HS_MAT_LAB, 'Y')) >= 10
                --AND (HS_MAT_LAB IS NULL OR TO_NUMBER(REGEXP_SUBSTR(HS_MAT_LAB, '^[0-9]+')) < 10)
            THEN 'Y'
            ELSE 'N'
         END AS CAN_CLEARABLE

   /* EMBEDDED OPTION */
        ,CASE
            WHEN UPPER(NVL(CNT_TYPO,' ')) LIKE '%CONTINGENT%'
            THEN 'MDET'
 
            WHEN UPPER(TRIM(NVL(CNT_TYPO,' '))) = 'SUST LINK DERIV'
            THEN 'OTHR'
 
            WHEN UPPER(NVL(CNT_TYPO,' ')) LIKE '%CANC%'
            THEN 'CANC'
 
            WHEN TRN_FMLY IN ('IRD','EQD','CRD')
             AND NVL(TRN_GRP,' ') <> 'OSWP'
             AND TP_CANCBY IS NOT NULL
            THEN 'CANC'
 
            WHEN H_PRODUCT = 'IRD_IRS'
             AND M_PKG_TYP_R = 'IR Rebate Swap'
             AND TRIM(CNT_TYPO) IS NULL
            THEN 'OTHR'
 
            WHEN M_PKG_TYP_R IN ('IR Floored Rate','IR Capped Rate')
            THEN 'OTHR'
 
            WHEN TRN_GRP = 'IRS'
             AND HS_UPI = 'InterestRate:Exotic'
             AND TRIM(M_PKG_TYP_R) IS NOT NULL
            THEN 'OTHR'
 
            WHEN UPPER(TRIM(NVL(HS_MUTUALP,' '))) = 'MANDATORY'
            THEN 'MDET'
 
            WHEN UPPER(TRIM(NVL(HS_MUTUALP,' '))) = 'OPTIONAL'
            THEN 'OPET'
 
            WHEN UPPER(NVL(M_HS_PCK_DSC,' ')) LIKE '%MET%'
              OR UPPER(NVL(M_HS_PCK_DSC,' ')) LIKE '%MANDATORY EARLY TERMINATION%'
            THEN 'MDET'
 
            WHEN UPPER(NVL(M_HS_PCK_DSC,' ')) LIKE '%OET%'
              OR UPPER(NVL(M_HS_PCK_DSC,' ')) LIKE '%OPTIONAL EARLY TERMINATION%'
            THEN 'OPET'
 
            WHEN UPPER(NVL(M_HS_PCK_DSC,' ')) LIKE '%EXTENDIBLE%'
              OR UPPER(TRIM(NVL(U_OPTTYPE,' '))) = 'EXTENDIBLE'
            THEN 'EXTD'
 
            WHEN UPPER(NVL(M_HS_PCK_DSC,' ')) LIKE '%OFFSETCLAUSE%'
            THEN 'OTHR'
 
            ELSE NULL
         END AS EMBEDDED_OPTION

        ,CASE
            /* Primary rule */
            WHEN TRN_FMLY = 'IRD' AND TRN_GRP = 'IRS' THEN
                CASE
                    WHEN TP_LENTDSP = 'LE_NBCGFLTD' AND TP_RTCCP02 > 0 THEN 'SLLR'
                    ELSE 'BYER'
                END
            WHEN H_IPAY < 0 OR TP_RTCCP02 < 0 THEN 'BYER'
            WHEN H_IPAY > 0 OR TP_RTCCP02 > 0 THEN 'SLLR'
        
            /* Fallback when D_IPAY = 0 */
            WHEN (DEAL_LEG = 1 OR TP_RTNBLEG = 1) AND H_BS = 'B' THEN 'SLLR'
            WHEN (DEAL_LEG = 1 OR TP_RTNBLEG = 1) AND H_BS = 'S' THEN 'BYER'
            WHEN H_BS = 'B' THEN 'BYER'
            WHEN H_BS = 'S' THEN 'SLLR'
        
            ELSE NULL
        END AS BASE_DIRECTION

                
    FROM PACKAGE_INDICATOR PI
)

-- Then modify your X_BASE CTE to join from TRADE_FLAGS instead of GTR_BASE_TRADES
,X_BASE AS (
    SELECT
        TF.NB,
        TF.THD_VALUE_DATE,
        FF.DT_LEG0,
        FF.LEGSTEP,
        FF.DATE2,
        LEAD(FF.DATE2) OVER (
            PARTITION BY TF.NB, TF.THD_VALUE_DATE, FF.DT_LEG0
            ORDER BY FF.LEGSTEP
        ) AS NEXT_DATE2
    FROM TRADE_FLAGS TF
    JOIN SDH.TB_FIXFLOW_MUREX FF
        ON FF.NB = TF.NB
        AND FF.FFL_VALUE_DATE = TF.THD_VALUE_DATE
    WHERE FF.DATE2 IS NOT NULL
        AND FF.LEGSTEP BETWEEN 1 AND 17
        AND TF.TRN_GRP <> 'RTRN'
),

X AS (

    SELECT

        NB,

        THD_VALUE_DATE,

        MAX(CASE

                WHEN DT_LEG0 = 0 AND LEGSTEP = 1 THEN

                    CASE

                        WHEN NEXT_DATE2 IS NULL THEN 'EXPI'

                        WHEN NEXT_DATE2 - DATE2 <= 3 THEN 'DAIL'

                        WHEN ROUND(MONTHS_BETWEEN(NEXT_DATE2, DATE2)) = 1 THEN 'MNTH'

                        ELSE 'EXPI'

                    END

            END) AS QTY_FREQUENCY_LEG_1,

        MAX(CASE

                WHEN DT_LEG0 = 1 AND LEGSTEP = 1 THEN

                    CASE

                        WHEN NEXT_DATE2 IS NULL THEN 'EXPI'

                        WHEN NEXT_DATE2 - DATE2 <= 3 THEN 'DAIL'

                        WHEN ROUND(MONTHS_BETWEEN(NEXT_DATE2, DATE2)) = 1 THEN 'MNTH'

                        ELSE 'EXPI'

                    END

            END) AS QTY_FREQUENCY_LEG_2,

        MAX(CASE

                WHEN DT_LEG0 = 0 AND LEGSTEP = 1 THEN

                    CASE

                        WHEN NEXT_DATE2 IS NULL THEN NULL

                        WHEN NEXT_DATE2 - DATE2 <= 3 THEN 1

                        WHEN ROUND(MONTHS_BETWEEN(NEXT_DATE2, DATE2)) = 1 THEN 1

                        ELSE NULL

                    END

            END) AS QTY_FREQ_MULT_LEG_1,
            
        MAX(CASE
                WHEN DT_LEG0 = 1 AND LEGSTEP = 1 THEN
                    CASE
                        WHEN NEXT_DATE2 IS NULL THEN NULL
                        WHEN NEXT_DATE2 - DATE2 <= 3 THEN 1
                        WHEN ROUND(MONTHS_BETWEEN(NEXT_DATE2, DATE2)) = 1 THEN 1
                        ELSE NULL
                    END
            END) AS QTY_FREQ_MULT_LEG_2
    FROM X_BASE
    GROUP BY NB, THD_VALUE_DATE

)
 


/* =====================================================================
    F I N A L     S E L E C T
    
A...Q, AB...Ai, AP, AQ, AT...BA
=======================================================================*/

SELECT
    TF.NB
    , TF.CLEARED
    ,TF.CAN_CLEARABLE
    ,TF.HS_MAT_LAB
------------------------------------------------------------------------------

    /* Clearing Exceptions and Exemptions - Counterparty 2 */
    ,CASE
        WHEN TF.CLEARED IN ('Y','I')
            OR TF.EMBEDDED_OPTION IS NOT NULL
            OR TF.TRN_FMLY IN ('CURR','COM','EQD')
            OR TF.TRN_GRP IN ('BOND','CF','TRS','RTRN','CDS')
        THEN NULL

        /* CA + US */
        WHEN TF.HS_REPROLCSA = 'Y'
            AND TF.HS_REPROLCFTC = 'Y'
            AND TF.CAN_CLEARABLE = 'Y'
        THEN
            CASE
                WHEN UPPER(TRIM(NVL(TF.M_MARGIN_IND,' '))) = 'AFFILIATE' THEN 'AFFL'
                WHEN TF.CLR_EX_END_USER_EXCEPTION = 'Y' THEN 'ENDU'
                WHEN TF.CLR_EX_SMALL_BK_EXEMPTION = 'Y' THEN 'SMBK'
                WHEN TF.CLR_EX_COOP_EXEMPTION = 'Y' THEN 'COOP'
                WHEN TF.CLR_EX_NO_ACT_LETTER = 'Y' THEN 'NOAL'
                WHEN TF.CLR_EX_OTHER_EXCEPTIONS = 'Y' THEN 'OTHR'
                ELSE NULL
            END
        /* CA only */
        WHEN TF.HS_REPROLCSA = 'Y'
            AND NVL(TF.HS_REPROLCFTC,' ') <> 'Y'
            AND TF.CAN_CLEARABLE = 'Y'
        THEN
            CASE
                WHEN UPPER(TRIM(NVL(TF.M_MARGIN_IND,' '))) = 'AFFILIATE' THEN 'AFFL'
                WHEN TF.CLR_EX_END_USER_EXCEPTION = 'Y' THEN 'OTHR'
                WHEN TF.CLR_EX_SMALL_BK_EXEMPTION = 'Y' THEN 'OTHR'
                WHEN TF.CLR_EX_COOP_EXEMPTION = 'Y' THEN 'OTHR'
                WHEN TF.CLR_EX_NO_ACT_LETTER = 'Y' THEN 'OTHR'
                WHEN TF.CLR_EX_OTHER_EXCEPTIONS = 'Y' THEN 'OTHR'
                ELSE NULL
            END

        ELSE NULL
    END AS CLEARING_CPTY_2    
------------------------------------------------------------------------------    
    ,'BSGEFEIOM18Y80CKCV46' AS "COUNTERPARTY_1"
------------------------------------------------------------------------------    
    ,CASE
        WHEN CPTY_LEI IS NOT NULL THEN CPTY_LEI
        ELSE
            'BSGEFEIOM18Y80CKCV46' || CPTY_NAME
    END AS COUNTERPARTY_2
------------------------------------------------------------------------------    
    ,CASE
        WHEN CPTY_LEI IS NOT NULL THEN
            DECODE(CPTY_ID_SOURCE_PRIV_LAW_IDENT, 'Y', 'PLID', 'LEID')
        WHEN CPTY_ID_SOURCE_NAT_PERS_IND = 'Y' THEN
            'NPID'
        ELSE
            NULL
    END AS PARTY_TYP2 --Counterparty 2 Identifier Source
------------------------------------------------------------------------------    
    ,'TRUE' AS FINANCIAL_ENTITY_1
------------------------------------------------------------------------------
    ,DECODE(FINANCIAL_ENTITY, 'Y', 'TRUE', 'FALSE') AS FINANCIAL_ENTITY_2
------------------------------------------------------------------------------
    ,CASE
        WHEN TRN_FMLY = 'EQD' AND TRN_GRP = 'EQS' THEN NULL
        WHEN TRN_FMLY = 'IRD' AND TRN_GRP = 'TRS' THEN NULL
        WHEN TRN_FMLY = 'IRD' AND TRN_GRP = 'RTRN' THEN NULL
        WHEN TRN_FMLY = 'EQD' AND TRN_GRP = 'RTRN' 
            AND (H_BS = 'S' OR (NVL(CLEARING_THRESHOLD_EXEEDED,'N') = 'N' AND NVL(CANADIAN_DERIVATIVE_STATUS,'N') = 'N'))  
            THEN NULL        
        WHEN TRN_FMLY = 'COM' AND TRN_GRP = 'SWAP' THEN NULL

        WHEN (
            TRN_FMLY IN ('EQD', 'CRD') 
            OR HS_UPI LIKE ('%Credit%')
            OR HS_UPI LIKE ('%Equity%') 
        ) 
        OR (TRN_FMLY = 'COM')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'OSWP')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'BOND')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'IRS' AND UPPER(HS_UPI) LIKE '%CREDIT%') 
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'CS' AND ( HS_UPI LIKE ('%Credit%') OR HS_UPI LIKE ('%Equity%')))             
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'CF')
        OR (TRN_FMLY = 'EQD' AND TRN_GRP = 'OPT')
        OR (TRN_FMLY = 'CURR' AND TRN_GRP <> 'FXD')
        THEN 
            CASE 
                WHEN H_BS = 'B' AND (TRN_FMLY <> 'IRD' OR HS_UPI NOT LIKE ('%Credit%')) THEN 'BSGEFEIOM18Y80CKCV46'        
                ELSE
                 CPTY_LEI --WHEN H_BS = 'S'    THEN
            END
        ELSE 
            NULL
    END AS BUYER_IDENTIFIER
------------------------------------------------------------------------------
    ,CASE
        WHEN TRN_FMLY = 'EQD' AND TRN_GRP = 'EQS' THEN NULL
        WHEN TRN_FMLY = 'IRD' AND TRN_GRP = 'TRS' THEN NULL
        WHEN TRN_FMLY = 'IRD' AND TRN_GRP = 'RTRN' THEN NULL--        WHEN TRN_FMLY = 'EQD' AND TRN_GRP = 'RTRN' AND H_BS = 'S' THEN NULL
        WHEN TRN_FMLY = 'EQD' AND TRN_GRP = 'RTRN' 
            AND (H_BS = 'S' OR (NVL(CLEARING_THRESHOLD_EXEEDED, 'N') = 'N' AND NVL(CANADIAN_DERIVATIVE_STATUS, 'N') = 'N'))
            THEN NULL        
        WHEN TRN_FMLY = 'COM' AND TRN_GRP = 'SWAP' THEN NULL
        WHEN (
            TRN_FMLY IN ('EQD', 'CRD')
            OR HS_UPI LIKE ('%Credit%')
            OR HS_UPI LIKE ('%Equity%')
        )
        OR (TRN_FMLY = 'COM')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'OSWP')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'BOND')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'IRS' AND UPPER(HS_UPI) LIKE '%CREDIT%')
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'CS' AND (HS_UPI LIKE ('%Credit%') OR HS_UPI LIKE ('%Equity%')))
        OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'CF')
        OR (TRN_FMLY = 'EQD' AND TRN_GRP = 'OPT')
        OR (TRN_FMLY = 'CURR' AND TRN_GRP <> 'FXD')
        THEN
            CASE
                WHEN H_BS = 'S' OR (TRN_FMLY = 'IRD' AND HS_UPI LIKE ('%Credit%')) THEN 'BSGEFEIOM18Y80CKCV46'        
                ELSE CPTY_LEI
            END
        ELSE
            NULL
    END AS SELLER_IDENTIFIER
------------------------------------------------------------------------------
    ,CASE
        WHEN (
                (TRN_FMLY = 'IRD' AND TRN_GRP IN ('OSWP','BOND','CF'))
             OR (TRN_FMLY = 'IRD' AND TRN_GRP = 'IRS' AND UPPER(NVL(HS_UPI,' ')) LIKE '%CREDIT%')
             OR (
                TRN_FMLY = 'IRD' AND TRN_GRP = 'CS'
                AND (
                    UPPER(NVL(HS_UPI,' ')) LIKE '%CREDIT%'
                    OR UPPER(NVL(HS_UPI,' ')) LIKE '%EQUITY%'    
                    )
                )
             OR (TRN_FMLY = 'EQD' AND TRN_GRP = 'OPT')
             OR (TRN_FMLY = 'CURR' AND TRN_GRP <> 'FXD')
             )
        THEN NULL
        WHEN (TRN_FMLY = 'EQD' AND TRN_GRP = 'EQS')
            OR TRN_FMLY IN ('CURR','IRD')
            OR (TRN_FMLY = 'EQD' AND TRN_GRP = 'RTRN' AND H_BS = 'S')
            OR (TRN_FMLY = 'COM' AND TRN_GRP = 'SWAP')--OR (TRN_FMLY = 'IRD' AND TRN_GRP IN ('TRS','RTRN','CS','IRS')            
        THEN
            CASE
                WHEN H_BS = 'B' THEN
                CASE
                    WHEN (TRN_FMLY = 'CURR' AND TRN_GRP = 'FXD' AND CANADA = 'Y')
                        OR (TRN_FMLY = 'IRD' AND TRN_GRP <> 'TRS') OR TRN_FMLY = 'COM' --OR CANADA <> 'Y')
                    THEN COALESCE(NULLIF(CPTY_LEI, ''), 'BSGEFEIOM18Y80CKCV46' || CPTY_NAME)
                    ELSE 'BSGEFEIOM18Y80CKCV46'
                END
                ELSE 
                    CASE
                        WHEN (TRN_FMLY = 'CURR' AND TRN_GRP = 'FXD' AND (CANADA <> 'Y' OR CANADIAN_DERIVATIVE_STATUS <> 'Y'))
                            OR (TRN_FMLY = 'IRD' AND TRN_GRP <> 'TRS') OR TRN_FMLY = 'COM' --OR CANADA <> 'Y')
                        THEN 'BSGEFEIOM18Y80CKCV46'
                        ELSE COALESCE(NULLIF(CPTY_LEI, ''), 'BSGEFEIOM18Y80CKCV46' || CPTY_NAME)
                    END
            END
        ELSE NULL
    END AS PAYER_IDENTIFIER_LEG_1

                --OR (TRN_FMLY <> 'IRD' AND TRN_FMLY <> 'COM')
                --AND NOT (TRN_FMLY = 'IRD' AND UPPER(NVL(HS_UPI,' ')) LIKE '%CREDIT%')

--    ,CASE
--        WHEN TRN_FMLY <> 'IRD' THEN NULL
--        WHEN H_DVCS = 'D' THEN 'BSGEFEIOM18Y80CKCV46'
--        ELSE CPTY_LEI
--    END AS PAYER_IDENTIFIER_LEG_1
------------------------------------------------------------------------------
    ,CASE
        WHEN TRN_FMLY <> 'IRD' THEN NULL
        WHEN H_DVCS = 'D' THEN CPTY_LEI
        ELSE 'BSGEFEIOM18Y80CKCV46'
    END AS PAYER_IDENTIFIER_LEG_2
------------------------------------------------------------------------------
    ,CASE
        WHEN TRN_FMLY <> 'IRD' THEN NULL
        WHEN H_DVCS = 'D' THEN CPTY_LEI
        ELSE 'BSGEFEIOM18Y80CKCV46'
    END AS RECEIVER_IDENTIFIER_LEG_1
------------------------------------------------------------------------------
    ,CASE
        WHEN TRN_FMLY <> 'IRD' THEN NULL
        WHEN H_DVCS = 'D' THEN 'BSGEFEIOM18Y80CKCV46'
        ELSE CPTY_LEI
    END AS RECEIVER_IDENTIFIER_LEG_2
------------------------------------------------------------------------------
    ,CASE
        WHEN FED_ENTITY_INDICATOR = 'Y' THEN 'TRUE'
        ELSE 'FALSE'
    END CPTY_2_FE_INDICATOR
------------------------------------------------------------------------------    
    ,CASE 
        WHEN M_BASKET_IND = 'Y' THEN 'TRUE'
        ELSE 'FALSE'
    END AS BASKET_INDICATOR
------------------------------------------------------------------------------        
    ,CASE
        WHEN M_VAL_METHOD IS NOT NULL THEN M_COMPRES_ID
        ELSE NULL
    END AS EVENT_IDENTIFIER --Event Identifier P
------------------------------------------------------------------------------        
    ,CASE
        WHEN M_VAL_METHOD IS NOT NULL THEN HS_EXEC_TIME
        ELSE NULL
    END AS EVENT_TIMESTAMP --Event Timestamp Q
------------------------------------------------------------------------------        
    ,CONCAT_CAPREM0  AS NOTI_AMOUNT_IE_ON_ASSOC_EFF_DATE_LEG_1 --Notional Amount in Effect on Associated Effective Date-Leg 1
------------------------------------------------------------------------------        
    ,CASE
        WHEN TRN_FMLY = 'CURR'
         AND TRN_GRP  = 'OPT'
        THEN
            CASE
                WHEN H_PUTCALL = 'C' THEN 
                    LPAD( TO_CHAR( ROUND(TP_IQTY, 2),'FM99999999999999999999999999.000000000000'), 39, '0' )                                
                WHEN H_PUTCALL = 'P'
                 AND (
                        NVL(TRN_TYPE, ' ') NOT IN ('ASN', 'BAR', 'BAR2')
                    OR (H_DVCS = 'D' AND TP_OBAR1 <> 0)
                    OR (H_DVCS = 'C' AND TP_OBAR1 = 0)
                     )
                THEN 
                    LPAD( TO_CHAR( ROUND(TP_QTYEQ, 2),'FM99999999999999999999999999.000000000000'), 39, '0' )                                
                ELSE NULL
            END
        ELSE NULL
    END AS CALL_AMOUNT_LEG_1 --Call Amount-Leg 1 AB
------------------------------------------------------------------------------            
    ,NULL AS CALL_AMOUNT_LEG_2
------------------------------------------------------------------------------                
    ,CASE
        WHEN TRN_FMLY = 'CURR'
         AND TRN_GRP  = 'OPT'
        THEN
            CASE
                WHEN H_PUTCALL = 'C' THEN DECODE(TP_FXSTNUM, 'CNH', 'CNY', TP_FXSTNUM)
                WHEN H_PUTCALL = 'P'
                 AND (
                        NVL(TRN_TYPE, ' ') NOT IN ('ASN', 'BAR', 'BAR2')
                    OR (H_DVCS = 'D' AND TP_OBAR1 <> 0)
                    OR (H_DVCS = 'C' AND TP_OBAR1 = 0)
                     )
                THEN  DECODE(TP_FXSTDEN, 'CNH', 'CNY', TP_FXSTDEN)
                ELSE NULL
            END
        ELSE NULL
    END AS CALL_CURR_LEG_1
------------------------------------------------------------------------------            
    ,NULL AS CALL_CURR_LEG_2
------------------------------------------------------------------------------            
    ,CASE
        WHEN TRN_FMLY = 'CURR'
         AND TRN_GRP  = 'OPT'
         AND H_TP_AE  IN ('D', 'E')
         AND (
                TRN_TYPE = 'SMP'
             OR (
                    TRN_TYPE IN ('ASN', 'BAR', 'BAR2')
                AND (
                       (H_DVCS = 'D' AND H_TP_AE = 'E')
                    OR (H_DVCS = 'C' AND H_TP_AE = 'D')
                    )
                )
             )
        THEN
            CASE
                WHEN H_PUTCALL = 'P' THEN
                    LPAD( TO_CHAR( ROUND(TP_IQTY, 2),'FM99999999999999999999999999.000000000000'), 39, '0' )                                
                WHEN H_PUTCALL = 'C' THEN 
                    LPAD( TO_CHAR( ROUND(TP_QTYEQ, 2),'FM99999999999999999999999999.000000000000'), 39, '0' )                                
                ELSE NULL
            END
        ELSE NULL
    END AS PUT_AMOUNT_LEG_1
------------------------------------------------------------------------------            
    ,NULL AS PUT_AMOUNT_LEG_2
------------------------------------------------------------------------------            
    ,CASE
        WHEN TRN_FMLY = 'CURR'
         AND TRN_GRP  = 'OPT'
         AND H_TP_AE  IN ('D', 'E')
         AND (
                TRN_TYPE = 'SMP'
             OR (
                    TRN_TYPE IN ('ASN', 'BAR', 'BAR2')
                AND (
                       (H_DVCS = 'D' AND H_TP_AE = 'E')
                    OR (H_DVCS = 'C' AND H_TP_AE = 'D')
                    )
                )
             )
        THEN
            CASE
                WHEN H_PUTCALL = 'P' THEN DECODE(TP_FXSTNUM, 'CNH', 'CNY', TP_FXSTNUM)
                WHEN H_PUTCALL = 'C' THEN DECODE(TP_FXSTDEN, 'CNH', 'CNY', TP_FXSTDEN)
                ELSE NULL
            END
        ELSE NULL
    END AS PUT_CURR_LEG_1
------------------------------------------------------------------------------    
    ,NULL AS PUT_CURR_LEG_2 --Put Currency-Leg 2 Ai
------------------------------------------------------------------------------    
    ,X.QTY_FREQUENCY_LEG_1 --Quantity Frequency-Leg 1 AL
------------------------------------------------------------------------------    
    ,X.QTY_FREQUENCY_LEG_2 --Quantity Frequency-Leg 2 AL
------------------------------------------------------------------------------    
    ,X.QTY_FREQ_MULT_LEG_1
------------------------------------------------------------------------------     
    ,X.QTY_FREQ_MULT_LEG_2
--===========================================================================--    
    ,CASE
        -- Equity
        WHEN HS_UPI LIKE 'Equity:%' THEN
            CASE
                WHEN HS_UPI LIKE '%Index%' OR HS_UPI LIKE '%Basket%' THEN 'IPNT'
                ELSE 'SHAS'
            END
        -- FX
        WHEN HS_UPI LIKE 'ForeignExchange:%' THEN 'ACCY'
        -- Interest Rate
        WHEN HS_UPI LIKE 'InterestRate:%' THEN 'ACCY'
        -- Credit
        WHEN HS_UPI LIKE 'Credit:%' THEN 'ACCY'
        -- Commodity
        WHEN HS_UPI LIKE 'Commodity:Energy:Oil:%' THEN 'BARL'
        WHEN HS_UPI LIKE 'Commodity:Energy:NatGas:%' THEN 'MBTU'
        WHEN HS_UPI LIKE 'Commodity:Energy:Elec:%' THEN 'MWHO'
        WHEN HS_UPI LIKE 'Commodity:Metals:Precious:%' THEN 'OZTR'
        WHEN HS_UPI LIKE 'Commodity:Metals:NonPrecious:%' THEN 'KILO'
        WHEN HS_UPI LIKE 'Commodity:Agricultural:%' THEN 'BUSL'
        ELSE 'OTHR'
    END AS QUANTITY_UNIT_MEASURE_LEG_1    
------------------------------------------------------------------------------    
    ,CASE
        WHEN HS_UPI IS NULL THEN NULL

        WHEN UPPER(HS_UPI) LIKE '%OPTION%'
         AND NVL(TRN_TYPE, ' ') <> 'SWAP'
        THEN NULL

        -- Never use SHAS/IPNT on Leg 2
        WHEN UPPER(HS_UPI) LIKE 'EQUITY:%' THEN NULL
        WHEN UPPER(HS_UPI) LIKE 'COMMODITY:ENERGY:NATGAS:%' THEN 'MBTU'
        WHEN UPPER(HS_UPI) LIKE 'COMMODITY:ENERGY:OIL:%' THEN 'BARL'
        WHEN UPPER(HS_UPI) LIKE 'COMMODITY:ENERGY:ELEC:%' THEN 'MWHO'
        WHEN UPPER(HS_UPI) LIKE 'COMMODITY:METALS:PRECIOUS:%' THEN 'OZTR'
        WHEN UPPER(HS_UPI) LIKE 'COMMODITY:METALS:NONPRECIOUS:%' THEN 'KILO'
        WHEN UPPER(HS_UPI) LIKE 'COMMODITY:AGRICULTURAL:GRAINSOILSEEDS:%' THEN 'BUSL'
        WHEN UPPER(HS_UPI) LIKE 'FOREIGNEXCHANGE:%' THEN 'ACCY'
        WHEN UPPER(HS_UPI) LIKE 'INTERESTRATE:%' THEN 'ACCY'
        WHEN UPPER(HS_UPI) LIKE 'CREDIT:%' THEN 'ACCY'
        ELSE 'OTHR'
    END AS QUANTITY_UNIT_MEASURE_LEG_2    
------------------------------------------------------------------------------    
    ,PACKAGE_INDICATOR_RAW AS PACKAGE_INDICATOR --Package Indicator
------------------------------------------------------------------------------    
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW = 'TRUE' THEN
            CASE
                WHEN H_PRODUCT = 'CURR_FXD_SWLEG' THEN TO_CHAR(CONTRACT_NUMBER)
                WHEN NVL(H_PKG_LOG_ID, '0') <> '0' THEN H_PKG_LOG_ID
                WHEN NVL(PACKAGE, 0) <> 0 THEN TO_CHAR(PACKAGE)
                WHEN M_PKG_TYP_R = 'FX Dual Ccy Capped' THEN TO_CHAR(CONTRACT_NUMBER)
                ELSE NULL
            END
        ELSE NULL
    END AS PACKAGE_ID --Package Identifier
------------------------------------------------------------------------------    
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW = 'TRUE'
             AND M_PKG_TYP_R = 'Price'
        THEN M_PKG_VAL_R
        ELSE NULL
    END AS PACKAGE_TRAN_PRICE --Package Transaction Price
------------------------------------------------------------------------------    
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW = 'TRUE'
             AND M_PKG_TYP_R = 'Price'
             AND M_PKG_NOTA_R = 'Monetory Amount'
        THEN M_PKG_CURR_R
        ELSE NULL
    END AS PACKAGE_TRAN_PRICE_CURR --Package Transaction Price Currency
------------------------------------------------------------------------------    
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW = 'FALSE' THEN NULL
        WHEN M_PKG_TYP_R <> 'Price' THEN NULL
        WHEN M_PKG_NOTA_R = 'Monetory Amount' THEN '1'
        WHEN M_PKG_NOTA_R = 'Decimal' THEN '3'                
        ELSE 'EROR'
    END AS PACKAGE_TRAN_PRICE_NOTAT --Package Transaction Price Notation    
------------------------------------------------------------------------------    
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW = 'TRUE'
             AND M_PKG_TYP_R = 'Spread'
        THEN M_PKG_VAL_R
        ELSE NULL
    END AS PACKAGE_TRAN_SPREAD --Package Transaction Spread
------------------------------------------------------------------------------     
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW = 'TRUE'
             AND M_PKG_TYP_R = 'Spread'
             --AND PACKAGE_NOTAT_SPREAD = '1'
        THEN M_PKG_CURR_R
        ELSE NULL
    END AS PACKAGE_TRAN_SPREAD_CURR --Package Transaction Spread Currency
------------------------------------------------------------------------------     
    ,CASE
        WHEN PACKAGE_INDICATOR_RAW <> 'TRUE' THEN NULL
        WHEN M_PKG_TYP_R <> 'Spread' THEN NULL
        WHEN M_PKG_NOTA_R = 'Monetory Amount' THEN '1'
        WHEN M_PKG_NOTA_R = 'Decimal' THEN '3'
        WHEN M_PKG_NOTA_R = 'Basis points' THEN '4'        
        ELSE NULL
    END AS PACKAGE_TRAN_SPREAD_NOTAT --Package Transaction Spread Notation
------------------------------------------------------------------------------    

/*============================================================================*/

FROM TRADE_FLAGS TF
LEFT JOIN X
    ON X.NB = TF.NB
    AND X.THD_VALUE_DATE = TF.THD_VALUE_DATE
;
--WHERE TF.MATCH_RN = 1;

