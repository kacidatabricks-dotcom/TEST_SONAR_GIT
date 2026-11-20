
from pyspark.sql import functions as F
from pyspark.sql.window import Window
#===================================================
# SUPPRESSION DES INCOHERENCES RELATIVES AUX CLIENTS
#====================================================


def suppression_incoherences_clients(df, controles, date_naissance,
                         date_creation, cle_primaire):

    df_temp = df

    # ------------------------------------------------
    # 1. Conversion et validation des dates
    # ------------------------------------------------

    df_temp = df_temp.withColumn(
        "__birth_date",
        F.expr(
            f"try_to_timestamp(cast(`{date_naissance}` as string))"
        )
    )

    df_temp = df_temp.withColumn(
        "__creation_date",
        F.expr(
            f"try_to_timestamp(cast(`{date_creation}` as string))"
        )
    )

    controles.extend([

        (
            F.col("__birth_date").isNull(),
            "Date de naissance invalide"
        ),

        (
            F.col("__creation_date").isNull(),
            "Date de création invalide"
        ),

        (
            F.to_date("__birth_date") > F.current_date(),
            "Date de naissance dans le futur"
        ),

        (
            F.to_date("__creation_date") > F.current_date(),
            "Date de création dans le futur"
        ),

        (
            F.col("__creation_date") < F.col("__birth_date"),
            "Date de création antérieure à la naissance"
        )
    ])

    # ------------------------------------------------
    # 2. Détection des doublons sur la clé primaire
    # ------------------------------------------------

    fenetre = Window.partitionBy(cle_primaire)

    df_temp = df_temp.withColumn(
        "__duplicate_count",
        F.count("*").over(fenetre)
    )

    controles.append((
        F.col("__duplicate_count") > 1,
        "Identifiant client dupliqué"
    ))

    # ------------------------------------------------
    # 3. Construction des motifs d'anomalies
    # ------------------------------------------------

    motifs = [
        F.when(condition, F.lit(message))
        for condition, message in controles
    ]

    df_temp = df_temp.withColumn(
        "_MOTIF_INCOHERENCE",
        F.concat_ws(" | ", *motifs)
    )

    # ------------------------------------------------
    # 4. Séparation des lignes valides et invalides
    # ------------------------------------------------

    df_rejetes = df_temp.filter(
        F.col("_MOTIF_INCOHERENCE") != ""
    )

    df_clean = df_temp.filter(
        F.col("_MOTIF_INCOHERENCE") == ""
    )

    # Suppression des colonnes techniques
    colonnes_techniques = [
        "__birth_date",
        "__creation_date",
        "__duplicate_count",
        "_MOTIF_INCOHERENCE"
    ]

    df_clean = df_clean.drop(*colonnes_techniques)

    return df_clean, df_rejetes




# ===================================================================================================
# FONCTION GENERIQUE DE SUPPRESSION DES INCOHERENCES POUR LES ENTITES CONTRATS, SINISTRES Et PAYMENTS
# ===================================================================================================

def suppression_incoherences(
    df,
    controles,
    colonnes_dates=None,
    cle_primaire=None,
    contrats_valides=None
):

    df_temp = df

    # --------------------------------------------------------
    # 1. Conversion sécurisée des dates
    # --------------------------------------------------------

    colonnes_dates = colonnes_dates or []

    for colonne in colonnes_dates:

        df_temp = df_temp.withColumn(
            f"__date_{colonne}",
            F.expr(
                f"coalesce(try_to_timestamp(`{colonne}`, 'yyyy-MM-dd'), "
                f"try_to_timestamp(`{colonne}`, 'yyyy-MM-dd HH:mm:ss'))"
            )
        )

        controles.append((
            F.col(f"__date_{colonne}").isNull(),
            f"Date invalide : {colonne}"
        ))

    # --------------------------------------------------------
    # 2. Vérification des doublons de clé primaire
    # --------------------------------------------------------

    if cle_primaire:

        fenetre = Window.partitionBy(cle_primaire)

        df_temp = df_temp.withColumn(
            "__nombre_doublons",
            F.count("*").over(fenetre)
        )

        controles.append((
            F.col("__nombre_doublons") > 1,
            f"Clé primaire dupliquée : {cle_primaire}"
        ))

    # --------------------------------------------------------
    # 3. Vérification des références aux contrats
    # --------------------------------------------------------

    if contrats_valides is not None:

        # Colonne de référence contrat propre à la source (AB ou AP)
        colonne_ref = next(
            (c for c in ("AB_POLICY_NUMBER", "AP_CONTRACT_REF")
             if c in contrats_valides.columns and c in df_temp.columns),
            None
        )

        if colonne_ref:

            policies = (
                contrats_valides
                .select(colonne_ref)
                .distinct()
                .withColumn("__policy_exists", F.lit(1))
            )

            df_temp = (
                df_temp.alias("d")
                .join(
                    policies.alias("p"),
                    F.col(f"d.{colonne_ref}") ==
                    F.col(f"p.{colonne_ref}"),
                    "left"
                )
                .select(
                    "d.*",
                    F.col("p.__policy_exists")
                )
            )

            controles.append((
                F.col("__policy_exists").isNull(),
                "Référence de contrat inexistante"
            ))

    # --------------------------------------------------------
    # 4. Construction des motifs de rejet
    # --------------------------------------------------------

    motifs = [
        F.when(condition, F.lit(message))
        for condition, message in controles
    ]

    df_temp = df_temp.withColumn(
        "_MOTIF_INCOHERENCE",
        F.concat_ws(" | ", *motifs)
    )

    # --------------------------------------------------------
    # 5. Séparation des lignes valides et invalides
    # --------------------------------------------------------

    df_invalides = df_temp.filter(
        F.col("_MOTIF_INCOHERENCE") != ""
    )

    df_valides = df_temp.filter(
        F.col("_MOTIF_INCOHERENCE") == ""
    )

    # Suppression des colonnes techniques du DataFrame propre
    colonnes_techniques = [
        f"__date_{c}" for c in colonnes_dates
    ] + ["__nombre_doublons", "__policy_exists", "_MOTIF_INCOHERENCE"]

    df_valides = df_valides.drop(
        *[c for c in colonnes_techniques if c in df_valides.columns]
    )

    df_invalides = df_invalides.drop(
        *[c for c in colonnes_techniques
          if c in df_invalides.columns and c != "_MOTIF_INCOHERENCE"]
    )

    return df_valides, df_invalides
