import os  # Pour la variable d'environnement NCBI_API_KEY
import streamlit as st
import pandas as pd
import io
import ast, json
import traceback

# Importer les fonctions et constantes partagées depuis utils.py
from utils import (
    get_scopus_data, get_openalex_data, get_pubmed_data, convert_to_dataframe,
    clean_doi, HalCollImporter, merge_rows_with_sources, get_authors_from_crossref,
    check_df, enrich_w_upw_parallel, add_permissions_parallel, deduce_todo,
    normalise, normalize_name, get_initial_form
)

# Importer la génération ZIP / XML (hal_xml_export.py)
from hal_xml_export import generate_zip_from_xmls, extract_authors_from_openalex_json, _safe_text, _ensure_list

# --- Définition de la liste des laboratoires (spécifique à cette application) ---
labos_list_rennes = [
    {
        "collection": "CAPHI", "scopus_id": "60105490", "openalex_id": "I4387152714", "openalex_raw":"(\"Centre Atlantique de Philosophie\") OR CAPHI OR UR7463 OR (\"UR 7463\") OR (\"UR 1270\") OR UR1270  OR (\"EA 1270\") OR EA1270",
        "pubmed_query": "(CAPHI[Affiliation]) OR (\"CENTRE ATLANTIQUE DE PHILOSOPHIE\"[Affiliation]) OR (\"EA 7463\" [Affiliation]) OR (EA7463[Affiliation]) OR (UR7463[Affiliation]) OR (\"UR 7463\"[Affiliation])"
    },
    {
        "collection": "ARENES", "scopus_id": "60105601", "openalex_id": "I4387155702", "openalex_raw":"UMR6051 OR \"Centre de recherches sur l'action politique en Europe\" OR CRAPE OR ARENES",
        "pubmed_query": "(ARENES[Affiliation]) OR (\"UMR6051\"[Affiliation]) OR (UMR 6051[Affiliation] OR OR (UMR CNRS 6051[Affiliation])"
    },
        {"collection": "CREAAH", "scopus_id": "60105602", "openalex_id": "I4387153012", "openalex_raw":"UMR6566 OR \"UMR 6566\" OR \"CNRS 6566\" OR \"Centre de recherche en archéologie archéosciences histoire\" OR CReAAH OR \"Archaeology Archaeoscience and History\"", "pubmed_query": ""},
    {
        "collection": "BIOSIT", "scopus_id": "", "openalex_id": "", "openalex_raw":"ImPACcell OR H2P2 OR mric OR Protim OR FAIIA OR Prism",
        "pubmed_query": "(FAIIA[affiliation]) OR (mric[affiliation]) OR (prism[affiliation]) OR (H2P2[affiliation]) OR (Protim[affiliation]) OR (ImPACcell[affiliation])"
    },
    {
        "collection": "BRM", "scopus_id": "60206583", "openalex_id": "I4387155446", "openalex_raw":"\"ARN bactériens et médecine\" OR \"Bacterial RNAs and Medicine\" OR \"ARN régulateurs bactériens et médecine\" OR \"Bacterial regulatory RNAs and Medicine\" OR U1230 OR UMR1230 OR U835 OR \"U 1230\" OR \"U 835\" OR \"UMR_S 1230\" OR \"UMR_S1230\" OR \"Bacterial RNAs Function Structure\" OR \"Fonction structure et inactivation\"",
        "pubmed_query": "(U835[affiliation]) OR (UMR_S1230[affiliation]) OR (UMR1230[affiliation]) OR (U1230[affiliation]) OR (\"ARN régulateurs bactériens et médecine\"[affiliation]) OR (\"Bacterial regulatory RNAs and Medicine\"[affiliation]) OR ((Tattevin[Author]) OR (Cattoir[Author]) OR (Revest[Author]) OR (Le Pabic[Author]) OR (Donnio[Author]) OR (Le Pabic[Author]) AND Rennes[affiliation])"
    },
    {
        "collection": "CIC", "scopus_id": "60105521", "openalex_id": "I4210116274", "openalex_raw":"\"CIC Rennes\" OR \"Unité dʼInvestigation Clinique de Rennes\" OR \"Centre dʼInvestigation Clinique de Rennes\" OR \"Rennes Clinical investigation\" OR CIC1414 OR CIC203 OR CIC0203 OR \"CIC 1414\" OR \"CIC 203\" OR \"CIC 0203\" OR \"CIC-INSERM 1414\"",
        "pubmed_query": "((\"INSERM 1414\"[Affiliation]) OR (INSERM-CIC-1414[Affiliation]) OR (CIC-1414[Affiliation]) OR (0203[Affiliation]) OR (1414[Affiliation]) OR (\"INSERM 0203\"[Affiliation]) OR (\"Unité dʼInvestigation Clinique Rennes\"[Affiliation]) OR (\"Centre dʼInvestigation Clinique* Rennes\"[Affiliation]) OR (\"Clinical Investigation Center Rennes\"[Affiliation]) OR (\"Rennes Clinical Investigation Center\"[Affiliation]) NOT (\"U 804\"[Affiliation]) NOT (U804[Affiliation]) NOT (CIC-IT[Affiliation]) AND (rennes[Affiliation])) NOT (Inria[Affiliation]) NOT (FORTH/ICE-HT[Affiliation])"
    },
    {
        "collection": "OSS", "scopus_id": "60138518", "openalex_id": "I4210090689", "openalex_raw":"ERL440 OR \"ERL 440\" OR COSS OR \"Oncogenesis Stress and Signaling\" OR U1242 OR \"U 1242\" OR UMR1242 OR \"UMR 1242\"",
        "pubmed_query": "((u1242[Affiliation]) OR (\"u 1242\"[Affiliation]) OR (umrs1242[Affiliation]) OR (\"umr_s 1242\"[Affiliation]) OR (\"Oncogenesis, Stress and Signaling\"[Affiliation]) OR (COSS[Affiliation]) OR (ERL440[Affiliation]) OR (\"ERL 440\"[Affiliation]) OR (ER440[Affiliation]) OR ((Hélène BELOEIL[Author]) OR (Julien Edeline[Author]) OR (Samer Kayal[Author]) OR (Astrid Lievre[Author]) OR (Remy Pedeux[Author]) OR (Laurent Sulpice[Author]) OR (Charles Ricordel[Author]) AND (Rennes[Affiliation]))"
    },
    {
        "collection": "ECOBIO", "scopus_id": "60105587", "openalex_id": "I4210087209", "openalex_raw":"UMR6553 OR \"UMR 6553\" OR \"Écosystèmes biodiversité évolution\" OR (ECOBIO AND France) OR \"Ecosystem Biodiversity Evolution\" NOT \"Laboratoire de Géographie Physique et Environnementale\"",
        "pubmed_query": "((ecobio[Affiliation]) OR (6553[Affiliation]) OR (\"Écosystèmes, biodiversité, évolution\"[Affiliation]) OR (\"Ecosystems, Biodiversity, Evolution\"[Affiliation]) AND (rennes[Affiliation])) OR (paimpont[Affiliation])"
    },
    {
        "collection": "ETHOS", "scopus_id": "60105604", "openalex_id": "I4387154707", "openalex_raw":"((UMR6552 OR \"UMR 6552\" OR \"Ethologie animale et humaine\" OR \"animal and human ethology\" OR \"Éthologie animale et humaine—UMR 6552\" OR ETHOS) AND (France))",
        "pubmed_query": "(UMR6552[Affiliation]) OR (\"UMR 6552\"[Affiliation]) OR (\"UMR* 6552\"[Affiliation]) OR (\"Ethologie animale et humaine\"[Affiliation]) OR (\"animal and human ethology\"[Affiliation])"
    },
    {
        "collection": "FOTON", "scopus_id": "60105599", "openalex_id": "I4210138837", "openalex_raw":"UMR6082 OR \"UMR 6082\" OR \"Fonctions Optiques pour les Technologies\" OR \"Optical Functions for Information Technology\" OR ENSSAT OR (FOTON AND France) NOT \"DEOS - Département Electronique Optronique et Signal\"",
        "pubmed_query": "(\"Fonctions Optiques pour les Technologies de l’information\"[Affiliation]) OR (UMR6082[Affiliation]) OR (UMR 6082[Affiliation])"
    },
    {
        "collection": "IETR", "scopus_id": "60105585", "openalex_id": "I4210100151", "openalex_raw":"(UMR6164 OR \"UMR 6164\" OR \"Institut d’Électronique et de Télécommunication de Rennes\" OR \"Institut d electronique et de Telecommunication de Rennes\" OR \"Institute of Electronics and Telecommunications of Rennes\" OR IETR OR \"Institut d'Électronique et des Technologies du numéRique\" OR \"IETR–UMR\") NOT (Nantes) NOT (\"Innovative Research Unit of Epithelial Transport and Regulation\"),display_name:!Batard|Bourlier|Charge|Chousseaud|Descamps|Diouris|El Assad|El Gibari|Feuvrie|Froppier|Ginot|Guiffard|Gundel|Le Bastard|Le Nours|Li Hong|Mahe|Motta Cruz|Pasquier|Pillement|Poulain|Razban|Wang",
        "pubmed_query": "(IETR[Affiliation]) OR (Institut d'Électronique et des Technologies du numéRique[Affiliation]) OR (UMR6164[Affiliation]) OR (UMR 6164[Affiliation]) OR (IETR Polytech[Affiliation]) OR (Institut d'Electronique et des Technologies du numéRique[Affiliation]) OR (Institut d'Électronique et de Télécommunications[Affiliation])"
    },
    {
        "collection": "IGDR", "scopus_id": "60105597", "openalex_id": "I4210127029", "openalex_raw":"ERL1305 OR \"ERL 1305\" OR UMR6290 OR \"UMR 6290\" OR IGDR OR \"Institut de génétique et développement de Rennes\" OR \"Institut de genetique et developpement de Rennes\" NOT \"Instituto de Geografía y Desarrollo Regional\"",
        "pubmed_query": "(IGDR[Affiliation]) OR (6290[Affiliation]) OR (\"Institut de génétique et développement de Rennes\"[Affiliation]) OR (\"Institute of Genetics and Development of Rennes\"[Affiliation]) OR (Lise BOUSSEMART[Author]) OR (Véronique DAVID[Author]) OR (Marie DE TAYRAC[Author]) OR (Virginie DELIGNIERE-GANDEMER[Author]) OR (Virginie GANDEMER[Author]) OR (Christèle DUBOURG[Author]) OR (Marie-Domininique GALIBERT[Author]) OR (Jean MOSSER[Author]) OR (Sylvie ODENT[Author]) NOT (Nikon[Affiliation])"
    },
    {
        "collection": "IPR", "scopus_id": "60105586", "openalex_id": "I4210109443", "openalex_raw":"UMR6251 OR \"UMR 6251\" OR \"CNRS 6251\" OR UMR-6251 OR \"Institut de physique de Rennes\" OR \"Institute of Physics of Rennes\" OR \"Rennes Institute of Physics\" OR LARMAUR OR \"Mecanique et Verres\" OR (IPR AND France) NOT (\"Institut Pierre Richet\" OR \"Intelligent Process Automation and Robotics\" OR \"Sant Joan de Deu\" OR \"Institut Polytechnique Rural de Formation et de Recherche Appliquee\" OR \"Institut Paris Region\")",
        "pubmed_query": "((IPR[Affiliation]) AND (rennes[Affiliation])) OR (\"Institut de Physique de Rennes\"[Affiliation]) OR (UMR6251[Affiliation]) OR (\"UMR 6251\"[Affiliation]) NOT (\"Institut Pierre Richet\"[Affiliation]) NOT (\"Intelligent Process Automation and Robotics\"[Affiliation]) NOT (\"Sant Joan de Déu\"[Affiliation])"
    },
    {
        "collection": "IRSET", "scopus_id": "60105594", "openalex_id": "I4210108239", "openalex_raw":"UMR1085 OR U1085 OR \"U 1085\" OR IRSET OR \"Research Institute for Environmental and Occupational Health\" OR \"Institut de Recherche en Santé Environnement et Travail\" OR \"Institut de Recherche en Sante Environnement et Travail\" OR \"IRSET-INSERM-1085\" OR \"Irset-UMR_S 1085\" OR \"UMR_S 1085\" OR \"Institute for Research in Health Environment and Work\" OR \"UMRS 1085\"  OR \"UMR 1085\" NOT (ISORG OR \"Julia Ester Gonzales Delgado de Loja\" OR \"Department of Speech Language and Hearing Sciences\"),institutions.id:!I157943965|I43526919|I4210159825| I101202996,display_name:!Kristensen David Møbjerg|!Kristensen David M,author.id:!A5042113315|A5034669762",
        "pubmed_query": "(irset[Affiliation]) OR (U1085[Affiliation]) OR (UMR1085[Affiliation]) OR (\"UMR-S 1085\"[Affiliation]) OR (\"UMR_S 1085\"[Affiliation]) OR (\"Institut de recherche en santé, environnement et travail\"[Affiliation]) OR (\"Research Institute for Environmental and Occupational Health\"[Affiliation]) OR (\"Institute for Research in Health Environment and Work\"[Affiliation]) OR (Marc-Antoine BELAUD-ROTUREAU[Author]) OR (Célia Ravel[Author]) OR (Nathalie RIOUX-LECLERCQ[Author]) OR (Cécile VIGNEAU[Author]) OR (Fréderic DUGAY[Author]) OR (Sylvie JAILLARD[Author]) OR (Romain MATHIEU[Author]) OR (Solène-Florence KAMMERER-JACQUET[Author]) OR (Jonathan CHEMOUNY[Author]) NOT (IRSET-Center[Affiliation]) NOT (Kristensen[Author]"
    },
    {
        "collection": "ISCR", "scopus_id": "60072944", "openalex_id": "I4210090783", "openalex_raw":"UMR6226 OR \"UMR 6226\" OR \"ISCR – UMR 6226\" OR ISCR OR \"ISCR-UMR\" OR \"ISCR-UMR6226\" OR \"Institut des sciences chimiques de Rennes\" OR \"Sciences chimiques de Rennes\" OR MACSE OR \"Chemical Institute of Rennes\" OR \"Glasses and Ceramics\" OR PNSCM OR \"Rennes National Higher School of Chemistry\" OR \"Laboratory of Glasses and Ceramics\" OR \"Rennes Institute of Chemical Science\" OR \"Rennes Institute for Chemical Science\" OR \"Rennes Institute for Chemical Sciences\" OR \"École nationale supérieure de chimie de Rennes\" OR ENSCR OR \"Institute of Chemical Sciences of Rennes\"",
        "pubmed_query": "((\"institut des sciences chimiques de rennes\"[Affiliation]) OR (6226[Affiliation]) OR (ISCR[Affiliation]) OR (ISCR-UMR[Affiliation]) OR (\"National Higher School of Chemistry\"[Affiliation]) OR (MACSE[Affiliation]) OR (CORINT[Affiliation]) OR (\"Glasses and Ceramics\"[Affiliation]) OR (\"Institute of Chemical Science\"[Affiliation]) OR (\"Institute for Chemical Science\"[Affiliation]) OR (\"Ecole nationale supérieure de chimie de Rennes\"[Affiliation]) OR (ENSCR[Affiliation]) AND (rennes[Affiliation]))"
    },
    {
        "collection": "LGCGM", "scopus_id": "60105557", "openalex_id": "I4387155956", "openalex_raw":"(\"Laboratory of Civil Engineering and Mechanical Engineering\" OR UR3913 OR \"Laboratoire de génie civil et génie mécanique\" OR \"Laboratoire de génie civil et génie mécanique\" OR LGCGM OR \"UR 3913\" OR EA3913 OR \"EA 3913\" OR \"Laboratoire de génie civil et de génie mécanique\" OR \"Structural Engineering Research Group Lab\") AND (France)",
        "pubmed_query": "(LGCGM[Affiliation]) OR (UR 3913[Affiliation]) OR (UR3913[Affiliation]) OR (EA 3913[Affiliation]) OR (EA3913[Affiliation]) OR (\"Laboratoire de génie civil et génie mécanique\"[Affiliation] OR ((Quang Huy Nguyen[Author]) OR (Maël Couchaux[Author]) OR (Fabrice Bernard[Author]) OR (Paul Byrne[Author]) OR (Amina Meslem[Author]) OR (Florence Collet[Author]) OR (Mohammed Hjiaj[Author]) OR (Piseth Heng[Author]) OR (Hugues Somja[Author]) OR (Siham Kamali-Bernard[Author]) OR (Balaji Raghavan[Author]) AND (rennes[Affiliation]))"
    },
    {
        "collection": "LTSI", "scopus_id": "60105589", "openalex_id": "I4210105651", "openalex_raw":"\"UMR-1099\" OR UMR1099 OR U1099 OR \"U 1099\" OR \"UMR 1099\" OR \"UMR_S 1099\" OR LTSI OR CRIBS OR \"CIC-IT 804\" OR \"CIC-IT804\" OR \"CIC-IT Rennes\" OR \"Laboratoire traitement du signal et de l'image\" OR \"Laboratoire traitement du signal et de l image\" OR \"Signal and Image Processing Laboratory\" OR \"Centre de Recherche en Information Biomédicale Sino-Français\" OR \"Centre de Recherche en Information Biomedicale Sino-Francais\" OR (medicis AND Rennes) NOT (BiSIPL OR SIPL OR \"Remote Sensing Signal and Image Processing Laboratory\" OR \"Biomedical Signal and Image Processing Laboratory\" OR \"Industrial Technologies and Services Laboratory\" OR \"Laboratoire de Recherches Technologies et Services Industriels\" OR \"KSIP lab\" OR \"Center for Research for Infant Birth and Survival\")",
        "pubmed_query": "(LTSI[Affiliation]) OR (U1099[Affiliation]) OR (UMR1099[Affiliation]) OR (UMR* 1099[Affiliation]) OR (CRIBS[Affiliation]) OR (Centre de recherche en information biomédicale sino-français[Affiliation]) OR (CIC-IT[Affiliation]) OR (U* 804[Affiliation]) OR (medicis[Affiliation]) OR (Signal and Image Processing Laboratory[Affiliation] OR (\"Laboratoire traitement du signal et de l'image\"[Affiliation]) OR (\"Centre de Recherche en Information Biomédicale Sino-Français\"[Affiliation])"
    },
    {
        "collection": "M2S", "scopus_id": "60105531", "openalex_id": "I4210160484", "openalex_raw":"\"UR 7470\" OR UR7470 OR UR1274 OR \"UR 1274\" OR \"EA 7470\" OR EA7470 OR EA1274 OR \"EA 1274\" OR M2S OR \"Laboratoire mouvement sport et santé\" OR (Movement Sport Health AND Rennes) ",
        "pubmed_query": "(UR* 7470[Affiliation]) OR (UR7470[Affiliation]) OR (UR* 1274[Affiliation]) OR (EA* 7470[Affiliation]) OR (EA7470[Affiliation]) OR (EA* 1274[Affiliation]) OR (EA1274[Affiliation]) OR (\"Laboratoire mouvement, sport et santé\"[Affiliation]) OR (\"Movement, Sport, Health\"[Affiliation]) OR (M2S[Affiliation])"
    },
    {
        "collection": "MOBIDIC", "scopus_id": "60105591", "openalex_id": "I4387154398", "openalex_raw":"mobidic OR u1236 OR micmac OR \"Microenvironment and B-cells\" OR UMR1236 OR u917 OR \"UMR_ S 1236\"",
        "pubmed_query": "(MOBIDIC[Affiliation]) OR (Microenvironment and B-cells: Immunopathology, Cell Differentiation, and Cancer[Affiliation]) OR (Microenvironment and B-cells and Cancer[Affiliation]) OR (micmac[Affiliation]) OR (Microenvironment, Cell Differentiation, Immunology and Cancer[Affiliation]) OR (UMR_S 1236[Affiliation]) OR (U1236[Affiliation]) OR (U* 1236[Affiliation]) OR (UMR_S1236[Affiliation]) OR (u917[Affiliation]) OR (U 917[Affiliation]) OR (UMR_S917[Affiliation]) OR (UMR_S 917[Affiliation]) OR ((Olivier Decaux[Author] OR Roch Houot[Author]) AND Rennes[Affiliation])) NOT (Educell Ltd[Affiliation]) NOT (MicMac Road[Affiliation]) NOT (Montpellier BioInformatics for Clinical Diagnosis[Affiliation]))"
    },
    {
        "collection": "NUMECAN", "scopus_id": "60112105", "openalex_id": "I4387156410", "openalex_raw":"UMR1317 OR (NUMECAN AND Rennes) OR (NUMECAN AND \"Saint-Gilles\") OR U1317 OR \"U 1317\" OR \"U-1317\" OR UMR1241 OR \"U 1241\" OR \"U-1241\" OR EA1254 OR \"EA 1254\" OR U991 OR \"U 991\" OR (\"Nutrition Métabolismes et Cancer\" AND Rennes)",
        "pubmed_query": "(UMR991[affiliation]) OR (UMR 991[affiliation]) OR (U991[affiliation]) OR (U* 991[affiliation]) OR (foie, métabolismes et cancer[affiliation]) OR (foie, metabolismes et cancer[affiliation]) OR (liver, metabolisms and cancer[affiliation]) OR (EA 1254[affiliation]) OR (EA1254[affiliation]) OR (microbiologie: risques infectieux[affiliation]) OR (U1317[Affiliation]) OR (U 1317[Affiliation]) OR (U-1317[Affiliation]) OR (UMR_S 1317[Affiliation]) OR (UMR 1317[Affiliation]) OR (U1241[Affiliation]) OR (U 1241[Affiliation]) OR (U-1241[Affiliation]) OR (UMR_S 1241[Affiliation]) OR (UMR 1241[Affiliation]) OR (UMR 1341[Affiliation]) OR (UMR INRA 1341[Affiliation]) OR (numecan[Affiliation]) OR (nutrition, métabolismes et cancer[Affiliation]) OR (nutrition, metabolismes et cancer[Affiliation]) OR (nutrition, metabolisms and cancer[Affiliation]) OR ((ARNAUD Alexis OR BELLANGER Amandine OR BUFFET-BATAILLON Sylvie OR MOIRAND Romain OR THIBAULT Ronan OR ARTRU Florent OR BENDAVID Claude OR BOUGUEN Guillaume OR CABILLIC Florian OR GICQUEL Thomas OR LE DARE Brendan OR LEBOUVIER Thomas OR MOREL Isabelle OR NESSELER Nicolas  OR PELLETIER Romain OR PEYRONNET Benoit OR RAYAR Michel OR  SIPROUDHIS Laurent OR GUGGENBUHL Pascal OR  BARDOU-JACQUET Edouard OR BONNET Fabrice OR DESCLOS-THEVENIAU Marie OR GARIN Etienne OR HAMDI-ROZE Houda OR LAINE Fabrice OR MEURIC Vincent OR RANGE Hélène OR ROBIN François OR ROPARS Mickael OR ROPERT Martine OR TURLIN Bruno) AND (Rennes[Affiliation]))"
    },
    {
        "collection": "SCANMAT", "scopus_id": "60138457", "openalex_id": "I4387156459", "openalex_raw":"SCANMAT OR UMS2001 OR \"UMS 2001\" OR \"Synthèse Caractérisations Analyses de la MATière\" OR UAR2025 OR \"UAR 2025\"",
        "pubmed_query": "(SCANMAT[affiliation]) OR (UMS2001[affiliation])"
    },
    {
        "collection": "CREM", "scopus_id": "60105603", "openalex_id": "I4210088544", "openalex_raw":"(UMR6211 OR \"UMR 6211\" OR CREM OR \"Centre de recherche en économie et management\" OR \"Centre de recherche en economie et management\" OR \"Center for Research in Economics and Management\" OR \"IGR-IAE\" OR IGR OR \"Institut de gestion de Rennes\") AND (Rennes OR Caen) NOT Villejuif NOT \"Centre de Recherche sur les Médiations\"",
        "pubmed_query": ""
    },
    {
        "collection": "VIPS2", "scopus_id": "60105580", "openalex_id": "I4387155754", "openalex_raw":"\"Valeurs Innovations Politiques Socialisations Sports\" OR VIPS2 OR VIPS OR \"UR 4636\" OR UR4636",
        "pubmed_query": "(VIPS2[Affiliation]) OR (Valeurs, Innovations, Politiques, Socialisations et Sports[Affiliation]) OR (UR 4636[Affiliation]) OR (UR4636[Affiliation])"
    },
    {
        "collection": "GR", "scopus_id": "60070475", "openalex_id": "i4210096833", "openalex_raw":"\"Géosciences Rennes\" OR \"Geosciences Rennes\" OR UMR6118 OR VIPS OR \"UMR 6118\" OR \"CNRS 6118\" OR UMR-6118",
        "pubmed_query": "(Geosciences Rennes[Affiliation]) OR (CNRS 6118[Affiliation]) OR (UMR 6118[Affiliation]) OR (UMR-6118[Affiliation]) OR (UMR6118[Affiliation])"
    },
    {
        "collection": "IRISA", "scopus_id": "60027031", "openalex_id": "i2802519937", "openalex_raw":"\"Institut de Recherche en Informatique et Systèmes Aléatoires\" OR \"Institut de Recherche en Informatique et Systemes Aleatoires\" OR UMR6074 OR UMR-6074 OR \"UMR 6074\" OR \"CNRS 6074\" OR \" Institute for Research in IT and Random Systems\" OR IRISA",
        "pubmed_query": "(Institut de Recherche en Informatique et Systèmes Aléatoires[Affiliation]) OR (Institute for Research in IT and Random Systems[Affiliation]) OR (IRISA[Affiliation]) OR (UMR6074[Affiliation]) OR (UMR 6074[Affiliation]) OR (UMR-6074[Affiliation]) OR (CNRS 6074[Affiliation])"
    },
    {
        "collection": "CDA-PR", "scopus_id": "60105488", "openalex_id": "i4387152641", "openalex_raw":"\"Centre de droit des affaires\" OR \"UR 3195\" OR UR 3195 OR UR-3195 OR \"EA 3195\" OR \"EA-3195\" OR EA3195",
        "pubmed_query": "(Centre de droit des affaires[Affiliation]) OR (UR 3195[Affiliation]) OR (UR3195[Affiliation]) OR (EA 3195[Affiliation]) OR (EA3195[Affiliation])"
    },
    {
        "collection": "IGEPP", "scopus_id": "60105596", "openalex_id": "i4210141755", "openalex_raw":"\"Institut de Génétique Environnement et Protection des Plantes\" OR \"Institut de Genetique Environnement et Protection des Plantes\" OR IGEPP OR UMR1349 OR \"UMR 1349\" OR \"INRAE 1349\" OR UMR-1349",
        "pubmed_query": "(IGEPP[Affiliation]) OR (Institut de Genetique Environnement et Protection des Plantes[Affiliation]) OR (UMR 1349[Affiliation]) OR (UMR1349[Affiliation]) OR (UMR-1349[Affiliation])"
    },
    {
        "collection": "IDPSP", "scopus_id": "60105593", "openalex_id": "i4387154572", "openalex_raw":"\"Institut du droit public et de la science politique\" OR IDPSP OR UR4640 OR \"UR 4640\" OR \"EA 4640\" OR UR-4640 OR EA4640",
        "pubmed_query": "(IDPSP[Affiliation]) OR (Institut du droit public et de la science politique[Affiliation]) OR (UR 4640[Affiliation]) OR (UR4640[Affiliation]) OR (UR-4640[Affiliation])"
    },
    {
        "collection": "IODE", "scopus_id": "60105639", "openalex_id": "i4210128017", "openalex_raw":"\"Institut de l'Ouest : Droit et Europe\" OR \"Institut de l Ouest Droit et Europe\" OR (IODE AND Rennes) OR UMR6262 OR \"Western Institute of Law and Europe\" OR \"UMR 6262\" OR \"CNRS 6262\" OR UMR-6262",
        "pubmed_query": "(Institut de l'Ouest : Droit et Europe[Affiliation]) OR (Western Institute of Law and Europe[Affiliation]) OR (UMR6262[Affiliation]) OR (UMR 6262[Affiliation]) OR (UMR-6262[Affiliation]) OR (UMR-6262[Affiliation] AND Rennes)"
    },
    {
        "collection": "EMPENN-R", "scopus_id": "60206629", "openalex_id": "i4387152452", "openalex_raw":"Empenn OR \"U 1228\" OR (VISAGES AND Rennes) OR UMRS1228 OR \"UMRS 1228\" OR \"UMR_S 1228\" OR \"UMR 1228\" OR UMR1228 OR UMR-1228",
        "pubmed_query": "(EMPENN[Affiliation]) OR (VISAGES[Affiliation] AND Rennes) OR (UMRS1228[Affiliation]) OR (UMRS 1228[Affiliation]) OR (UMR_S 1228[Affiliation]) OR (UMR1228[Affiliation]) OR (UMR 1228[Affiliation]) OR (UMR-1228[Affiliation])"
    },
    {
        "collection": "IRMAR", "scopus_id": "60105640", "openalex_id": "i4210161663", "openalex_raw":"IRMAR OR \"Institut de recherche mathématique de Rennes\" OR UMR6625 OR \"Institut de recherche mathematique de Rennes\" OR \"Mathematics Research Institute of Rennes\" OR \"UMR 6625\" OR UMR-6625 OR \"CNRS 6625\"",
        "pubmed_query": "(IRMAR[Affiliation]) OR (Institut de recherche mathématique de Rennes[Affiliation] AND Rennes) OR (Mathematics Research Institute of Rennes[Affiliation]) OR (UMR 6625[Affiliation]) OR (UMR6625[Affiliation]) OR (UMR-6625[Affiliation]) OR (CNRS 6625[Affiliation])"
    },
]
labos_df_rennes_global = pd.DataFrame(labos_list_rennes)


# Fonction pour ajouter le menu de navigation (spécifique à cette app)
def add_sidebar_menu():
    st.sidebar.header("À Propos")
    st.sidebar.info(
    """
    **c2LabHAL - Version Université de Rennes** :
    Cette version est préconfigurée pour les laboratoires de l'Université de Rennes.
    Sélectionnez un laboratoire dans la liste pour lancer la comparaison de ses publications
    (Scopus, OpenAlex, PubMed) avec sa collection HAL. c2LabHAL est une application créée par Guillaume Godet (Nantes Univ)
    """
)
    st.sidebar.markdown("---")

    st.sidebar.header("Autres applications c2LabHAL")
    st.sidebar.markdown("📖 [c2LabHAL - Application Principale](https://c2labhal.streamlit.app/)")
    st.sidebar.markdown("📄 [c2LabHAL version CSV](https://c2labhal-csv.streamlit.app/)")


    st.sidebar.markdown("---")
    
    st.sidebar.markdown("Présentation du projet :")
    st.sidebar.markdown("[📊 Voir les diapositives](https://slides.com/guillaumegodet/deck-d5bc03#/2)")
    st.sidebar.markdown("Code source :")
    st.sidebar.markdown("[🐙 Voir sur GitHub](https://github.com/GuillaumeGodet/c2labhal)")


def main():
    st.set_page_config(page_title="c2LabHAL - Rennes", layout="wide")

    # Initialisation des clés session (protège contre les reruns)
    st.session_state.setdefault('zip_buffer', None)
    st.session_state.setdefault('publications_list', [])
    st.session_state.setdefault('last_result_df', None)       # contient la table résultat sérialisée (list of dicts)
    st.session_state.setdefault('last_collection', None)      # collection traitée (nom)
    st.session_state.setdefault('zip_buffer', None)           # bytes du zip généré

    add_sidebar_menu() 

    st.title("🥎 c2LabHAL - Version Université de Rennes")
    st.subheader("Comparez les publications d’un laboratoire de l'Université de Rennes avec sa collection HAL")

    labo_choisi_nom_rennes = st.selectbox(
        "Choisissez une collection HAL de laboratoire (Université de Rennes) :", 
        sorted(labos_df_rennes_global['collection'].unique())
    )

    labo_selectionne_details_rennes = labos_df_rennes_global[labos_df_rennes_global['collection'] == labo_choisi_nom_rennes].iloc[0]
    collection_a_chercher_rennes = labo_selectionne_details_rennes['collection']
    scopus_lab_id_rennes = labo_selectionne_details_rennes.get('scopus_id', '') 
    openalex_institution_id_rennes = labo_selectionne_details_rennes.get('openalex_id', '')
    openalex_institution_raw_rennes = labo_selectionne_details_rennes.get('openalex_raw', '') # ajout Laurent
    pubmed_query_labo_rennes = labo_selectionne_details_rennes.get('pubmed_query', '')

    scopus_api_key_secret_rennes = st.secrets.get("SCOPUS_API_KEY")
    pubmed_api_key_secret_rennes = st.secrets.get("PUBMED_API_KEY")

    col1_dates_rennes, col2_dates_rennes = st.columns(2)
    with col1_dates_rennes:
        start_year_rennes = st.number_input("Année de début", min_value=1900, max_value=2100, value=2020, key="rennes_start_year")
    with col2_dates_rennes:
        end_year_rennes = st.number_input("Année de fin", min_value=1900, max_value=2100, value=pd.Timestamp.now().year, key="rennes_end_year")

    with st.expander("🔧 Options avancées pour les auteurs"):
        fetch_authors_rennes = st.checkbox("🧑‍🔬 Récupérer les auteurs via Crossref (peut ralentir)", value=False, key="rennes_fetch_authors_cb")
        compare_authors_rennes = False
        uploaded_authors_file_rennes = None
        if fetch_authors_rennes:
            compare_authors_rennes = st.checkbox("🔍 Comparer les auteurs avec une liste de chercheurs", value=False, key="rennes_compare_authors_cb")
            if compare_authors_rennes:
                uploaded_authors_file_rennes = st.file_uploader(
                    "📤 Téléversez un fichier CSV de chercheurs (colonnes: 'collection', 'prénom nom')", 
                    type=["csv"], 
                    key="rennes_upload_authors_fu",
                    help="Le fichier CSV doit avoir une colonne 'collection' (code de la collection HAL) et une colonne avec les noms des chercheurs."
                )
    
    progress_bar_rennes = st.progress(0)
    progress_text_area_rennes = st.empty() # Correction: Suffixe _rennes ajouté

    if st.button(f"🚀 Lancer la recherche pour {collection_a_chercher_rennes}"):
        if pubmed_api_key_secret_rennes and pubmed_query_labo_rennes:
            os.environ['NCBI_API_KEY'] = pubmed_api_key_secret_rennes

        scopus_df_rennes = pd.DataFrame()
        openalex_df_rennes = pd.DataFrame()
        pubmed_df_rennes = pd.DataFrame()

        # --- Étape 1 : Récupération OpenAlex (par id institution) ---
        if openalex_institution_id_rennes:
            with st.spinner(f"Récupération OpenAlex pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 1/9 : Récupération des données OpenAlex...")
                progress_bar_rennes.progress(5)
                openalex_query_complet_rennes = f"authorships.institutions.id:{openalex_institution_id_rennes},publication_year:{start_year_rennes}-{end_year_rennes}"
                openalex_data_rennes = get_openalex_data(openalex_query_complet_rennes, max_items=5000)
                if openalex_data_rennes:
                    openalex_df_rennes = convert_to_dataframe(openalex_data_rennes, 'openalex')
                    openalex_df_rennes['Source title'] = openalex_df_rennes.apply(
                        lambda row: row.get('primary_location', {}).get('source', {}).get('display_name') if isinstance(row.get('primary_location'), dict) and row['primary_location'].get('source') else None, axis=1
                    )
                    openalex_df_rennes['Date'] = openalex_df_rennes.get('publication_date', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['doi'] = openalex_df_rennes.get('doi', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['id'] = openalex_df_rennes.get('id', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['Title'] = openalex_df_rennes.get('title', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    cols_to_keep_rennes = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                    openalex_df_rennes = openalex_df_rennes[[col for col in cols_to_keep_rennes if col in openalex_df_rennes.columns]]
                    if 'doi' in openalex_df_rennes.columns:
                        openalex_df_rennes['doi'] = openalex_df_rennes['doi'].apply(clean_doi)
                st.success(f"{len(openalex_df_rennes)} publications OpenAlex trouvées pour {collection_a_chercher_rennes}.")
        progress_bar_rennes.progress(10)

        # --- Étape 1b : Récupération OpenAlex (raw affiliation strings) ---
        if openalex_institution_raw_rennes:
            with st.spinner(f"Récupération OpenAlex (raw) pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 1/9 : Récupération des données OpenAlex (raw)...")
                progress_bar_rennes.progress(12)
                openalex_query_complet_rennes = f"raw_affiliation_strings.search:{openalex_institution_raw_rennes},publication_year:{start_year_rennes}-{end_year_rennes}"
                openalex_data_rennes = get_openalex_data(openalex_query_complet_rennes, max_items=5000)

                if openalex_data_rennes:
                    openalex_df_rennes = convert_to_dataframe(openalex_data_rennes, 'openalex')
                    openalex_df_rennes['Source title'] = openalex_df_rennes.apply(
                        lambda row: row.get('primary_location', {}).get('source', {}).get('display_name')
                        if isinstance(row.get('primary_location'), dict) and row['primary_location'].get('source')
                        else None,
                        axis=1
                    )
                    openalex_df_rennes['Date'] = openalex_df_rennes.get('publication_date', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['doi'] = openalex_df_rennes.get('doi', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['id'] = openalex_df_rennes.get('id', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    openalex_df_rennes['Title'] = openalex_df_rennes.get('title', pd.Series(index=openalex_df_rennes.index, dtype='object'))
                    cols_to_keep_rennes = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                    openalex_df_rennes = openalex_df_rennes[[col for col in cols_to_keep_rennes if col in openalex_df_rennes.columns]]

                    if 'doi' in openalex_df_rennes.columns:
                        openalex_df_rennes['doi'] = openalex_df_rennes['doi'].apply(clean_doi)

                    # 🧩 ---- Bloc d’enrichissement à insérer ici ----
                    def enrich_with_openalex_authors(openalex_results):
                        publications = []
                        for pub in openalex_results:
                            try:
                                authors_data = extract_authors_from_openalex_json(pub)
                            except Exception as e:
                                st.warning(f"Erreur dans extract_authors_from_openalex_json pour {pub.get('id', 'inconnu')}: {e}")
                                authors_data = []

                            st.write(f"OpenAlex: '{pub.get('title', '')[:80]}' → {len(authors_data)} auteurs extraits")

                            institutions = []
                            for a in authors_data:
                                for aff in a.get("raw_affiliations", []):
                                    institutions.append({
                                        "display_name": aff,
                                        "type": "institution"
                                    })
                            unique_institutions = [dict(t) for t in {tuple(d.items()) for d in institutions}]

                            publications.append({
                                "Title": pub.get("title"),
                                "doi": pub.get("doi"),
                                "Source title": pub.get("primary_location", {}).get("source", {}).get("display_name"),
                                "Date": pub.get("publication_date"),
                                "authors": authors_data,
                                "institutions": unique_institutions,
                                "Data source": "openalex"
                            })
                        return publications

                    # Application de la fonction d’enrichissement
                    enriched_publications_rennes = enrich_with_openalex_authors(openalex_data_rennes)
                    st.session_state['openalex_publications_raw'] = enriched_publications_rennes
                    st.info(f"✅ Données OpenAlex enrichies et stockées ({len(enriched_publications_rennes)} publications)")

                    openalex_df_rennes = pd.DataFrame(enriched_publications_rennes)
                    st.write("🧩 Données OpenAlex enrichies :", openalex_df_rennes.head(2))
                    # 🧩 ---- Fin du bloc d’enrichissement ----

                    # ✅ Et c’est ici que tu gardes ta ligne d’origine :
                    st.success(f"{len(openalex_df_rennes)} publications OpenAlex trouvées pour {collection_a_chercher_rennes}.")

        progress_bar_rennes.progress(15)

        # --- Étape 2 : Récupération PubMed ---
        if pubmed_query_labo_rennes: 
            with st.spinner(f"Récupération PubMed pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 2/9 : Récupération des données PubMed...")
                progress_bar_rennes.progress(20)
                pubmed_full_query_rennes = f"({pubmed_query_labo_rennes}) AND ({start_year_rennes}/01/01[Date - Publication] : {end_year_rennes}/12/31[Date - Publication])"
                pubmed_data_rennes = get_pubmed_data(pubmed_full_query_rennes, max_items=5000)
                if pubmed_data_rennes:
                    pubmed_df_rennes = pd.DataFrame(pubmed_data_rennes)
                st.success(f"{len(pubmed_df_rennes)} publications PubMed trouvées pour {collection_a_chercher_rennes}.")
        else:
            st.info(f"Aucune requête PubMed configurée pour {collection_a_chercher_rennes}.")
        progress_bar_rennes.progress(20)

        # --- Étape 3 : Récupération Scopus ---
        if scopus_lab_id_rennes and scopus_api_key_secret_rennes:
            with st.spinner(f"Récupération Scopus pour {collection_a_chercher_rennes}..."):
                progress_text_area_rennes.info("Étape 3/9 : Récupération des données Scopus...")
                progress_bar_rennes.progress(25)
                scopus_query_complet_rennes = f"AF-ID({scopus_lab_id_rennes}) AND PUBYEAR > {start_year_rennes - 1} AND PUBYEAR < {end_year_rennes + 1}"
                scopus_data_rennes = get_scopus_data(scopus_api_key_secret_rennes, scopus_query_complet_rennes, max_items=5000)
                if scopus_data_rennes:
                    scopus_df_raw_rennes = convert_to_dataframe(scopus_data_rennes, 'scopus')
                    required_scopus_cols_rennes = {'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate'}
                    if required_scopus_cols_rennes.issubset(scopus_df_raw_rennes.columns):
                        scopus_df_rennes = scopus_df_raw_rennes[['Data source', 'dc:title', 'prism:doi', 'dc:identifier', 'prism:publicationName', 'prism:coverDate']].copy()
                        scopus_df_rennes.columns = ['Data source', 'Title', 'doi', 'id', 'Source title', 'Date']
                        if 'doi' in scopus_df_rennes.columns:
                            scopus_df_rennes['doi'] = scopus_df_rennes['doi'].apply(clean_doi)
                    else:
                        st.warning(f"Données Scopus incomplètes pour {collection_a_chercher_rennes}. Scopus sera ignoré.")
                        scopus_df_rennes = pd.DataFrame()
                st.success(f"{len(scopus_df_rennes)} publications Scopus trouvées pour {collection_a_chercher_rennes}.")
        elif scopus_lab_id_rennes and not scopus_api_key_secret_rennes:
            st.warning(f"L'ID Scopus est fourni pour {collection_a_chercher_rennes} mais la clé API Scopus n'est pas configurée. Scopus sera ignoré.")
        progress_bar_rennes.progress(30)

        # --- Étape 4 : Combinaison des données ---
        progress_text_area_rennes.info("Étape 4/9 : Combinaison des données sources...")
        combined_df_rennes = pd.concat([scopus_df_rennes, openalex_df_rennes, pubmed_df_rennes], ignore_index=True)

        if combined_df_rennes.empty:
            st.error(f"Aucune publication récupérée pour {collection_a_chercher_rennes}. Vérifiez la configuration du laboratoire.")
            st.stop()
        
        if 'doi' not in combined_df_rennes.columns:
            combined_df_rennes['doi'] = pd.NA
        combined_df_rennes['doi'] = combined_df_rennes['doi'].astype(str).str.lower().str.strip().replace(['nan', 'none', 'NaN', ''], pd.NA, regex=False)


        # --- Étape 5 : Fusion des lignes en double ---
        progress_text_area_rennes.info("Étape 5/9 : Fusion des doublons...")
        progress_bar_rennes.progress(40)
        
        with_doi_df_rennes = combined_df_rennes[combined_df_rennes['doi'].notna()].copy()
        without_doi_df_rennes = combined_df_rennes[combined_df_rennes['doi'].isna()].copy()
        
        
        merged_data_doi_rennes = pd.DataFrame()
        if not with_doi_df_rennes.empty:
            merged_data_doi_rennes = with_doi_df_rennes.groupby('doi', as_index=False).apply(merge_rows_with_sources)
            if 'doi' not in merged_data_doi_rennes.columns and merged_data_doi_rennes.index.name == 'doi':
                merged_data_doi_rennes.reset_index(inplace=True)
            if isinstance(merged_data_doi_rennes.columns, pd.MultiIndex):
                 merged_data_doi_rennes.columns = merged_data_doi_rennes.columns.droplevel(0)
        
       
        merged_data_no_doi_rennes = pd.DataFrame()
        if not without_doi_df_rennes.empty:
            merged_data_no_doi_rennes = without_doi_df_rennes.copy() 
        
       
        final_merged_data_rennes = pd.concat([merged_data_doi_rennes, merged_data_no_doi_rennes], ignore_index=True)

        if final_merged_data_rennes.empty:
            st.error(f"Aucune donnée après fusion pour {collection_a_chercher_rennes}.")
            st.stop()
        st.success(f"{len(final_merged_data_rennes)} publications uniques après fusion pour {collection_a_chercher_rennes}.")
        progress_bar_rennes.progress(50)

        # --- Étape 6 : Comparaison HAL ---
        coll_df_hal_rennes = pd.DataFrame()
        with st.spinner(f"Importation de la collection HAL '{collection_a_chercher_rennes}'..."):
            progress_text_area_rennes.info(f"Étape 6a/9 : Importation de la collection HAL '{collection_a_chercher_rennes}'...")
            coll_importer_rennes_obj = HalCollImporter(collection_a_chercher_rennes, start_year_rennes, end_year_rennes)
            coll_df_hal_rennes = coll_importer_rennes_obj.import_data()
            if coll_df_hal_rennes.empty:
                st.warning(f"Collection HAL '{collection_a_chercher_rennes}' vide ou non chargée.")
            else:
                st.success(f"{len(coll_df_hal_rennes)} notices HAL pour {collection_a_chercher_rennes}.")
        
        progress_text_area_rennes.info("Étape 6b/9 : Comparaison avec les données HAL...")
        result_df_rennes = check_df(final_merged_data_rennes.copy(), coll_df_hal_rennes, progress_bar_st=progress_bar_rennes, progress_text_st=progress_text_area_rennes)
        st.success(f"Comparaison HAL pour {collection_a_chercher_rennes} terminée.")

        # --- Bloc d'enrichissement + filtrage + sauvegarde en session (placer ici, juste après la construction de result_df_rennes) ---

        def _normalize_doi_for_map(v):
            if not v:
                return ""
            s = str(v).strip().lower()
            for prefix in ("https://doi.org/", "http://doi.org/", "doi:", "doi.org/"):
                s = s.replace(prefix, "")
            return s
        
        # 1) Prépare la map OpenAlex (si disponible)
        openalex_list = st.session_state.get('openalex_publications_raw', []) or []
        oa_map = {}
        if openalex_list:
            for p in openalex_list:
                d = _normalize_doi_for_map(p.get('doi'))
                if d:
                    oa_map[d] = p
        
        st.write(f"DEBUG: OpenAlex en session: {len(openalex_list)} entrées ; map doi keys: {len(oa_map)}")
        
        # 2) Injecte auteurs/institutions dans result_df_rennes (créé plus haut)
        merged_records = []
        enriched_count = 0
        for rec in result_df_rennes.to_dict(orient='records'):
            doi_norm = _normalize_doi_for_map(rec.get('doi'))
            # par défaut, mettre listes vides (évite None)
            rec['authors'] = []
            rec['institutions'] = []
            if doi_norm and doi_norm in oa_map:
                oa = oa_map[doi_norm]
                # OA doit contenir 'authors' en list et 'institutions' en list — s'ils sont JSON string on tente de parser
                rec['authors'] = oa.get('authors') or []
                rec['institutions'] = oa.get('institutions') or []
                enriched_count += 1
            merged_records.append(rec)
        
        result_df_rennes = pd.DataFrame(merged_records)
        st.write(f"DEBUG: fusion OpenAlex -> result_df_rennes : {enriched_count} lignes enrichies sur {len(result_df_rennes)}")
        
        # 3) Filtrage final: ne garder QUE les publications hors HAL / hors collection
        if 'Statut_HAL' in result_df_rennes.columns:
            mask_non_hal = result_df_rennes['Statut_HAL'].fillna("").astype(str).isin(
                ["Hors HAL", "Dans HAL mais hors de la collection"]
            )
            filtered_df = result_df_rennes[mask_non_hal].copy()
            st.info(f"📦 Filtrage: {len(filtered_df)} publications retenues pour export (Statut_HAL hors/ hors-collection).")
        else:
            filtered_df = result_df_rennes.copy()
            st.warning("⚠️ Colonne 'Statut_HAL' absente — aucun filtrage appliqué.")
        
        # 4) Sanitize minimal des structures authors/institutions pour éviter erreurs lors du XML
        def safe_authors_field(a):
            if not a:
                return []
            if isinstance(a, str):
                try:
                    import json
                    parsed = json.loads(a)
                    a = parsed
                except Exception:
                    # si chaîne simple "Nom1; Nom2" -> split virgules
                    parts = [x.strip() for x in a.replace(';', ',').split(',') if x.strip()]
                    return [{"name": p, "orcid": "", "raw_affiliations": []} for p in parts]
            if isinstance(a, dict):
                return [a]
            if isinstance(a, list):
                clean = []
                for it in a:
                    if isinstance(it, dict):
                        clean.append({
                            "name": _safe_text(it.get("name") or it.get("raw_author_name") or it.get("display_name") or ""),
                            "orcid": _safe_text(it.get("orcid") or it.get("author", {}).get("orcid") or ""),
                            "raw_affiliations": _ensure_list(it.get("raw_affiliations") or it.get("raw_affiliation_strings") or it.get("institutions") or [])
                        })
                    elif isinstance(it, str):
                        clean.append({"name": _safe_text(it), "orcid": "", "raw_affiliations": []})
                return clean
            return []
        
        def safe_insts_field(i):
            if not i:
                return []
            if isinstance(i, str):
                return [{"display_name": _safe_text(i), "ror": ""}]
            if isinstance(i, dict):
                return [ {
                    "display_name": _safe_text(i.get("display_name", "")),
                    "ror": _safe_text(i.get("ror", "")),
                    "type": i.get("type","institution"),
                    "country": i.get("country","")
                } ]
            if isinstance(i, list):
                clean = []
                for it in i:
                    if isinstance(it, dict):
                        clean.append({
                            "display_name": _safe_text(it.get("display_name","") or it.get("name","")),
                            "ror": _safe_text(it.get("ror","")),
                            "type": it.get("type","institution"),
                            "country": it.get("country","")
                        })
                    elif isinstance(it, str):
                        clean.append({"display_name": _safe_text(it), "ror": "", "type":"institution", "country":""})
                return clean
            return []
        
        # Appliquer sanitation sur filtered_df
        sanitized = []
        for rec in filtered_df.to_dict(orient='records'):
            rec['authors'] = safe_authors_field(rec.get('authors'))
            rec['institutions'] = safe_insts_field(rec.get('institutions'))
            sanitized.append(rec)
        filtered_df = pd.DataFrame(sanitized)
        
        # 5) Sauvegarde incontournable pour l'export ZIP : on met la version FILTRÉE en session
        st.session_state['last_result_df_filtered'] = filtered_df.to_dict(orient='records')
        # on conserve aussi la version complète si besoin
        st.session_state['last_result_df_full'] = result_df_rennes.to_dict(orient='records')
        
        # Debug: afficher 3 éléments (aperçu) — très utile pour vérifier avant d'appuyer sur ZIP
        if len(filtered_df) > 0:
            st.write("🔍 Exemple (filtered_df[0]) :")
            try:
                st.json(filtered_df.iloc[0].to_dict())
            except Exception:
                st.write(filtered_df.iloc[0].to_dict())


        # --- Étape 7 : Enrichissement Unpaywall ---
        with st.spinner(f"Enrichissement Unpaywall pour {collection_a_chercher_rennes}..."):
            progress_text_area_rennes.info("Étape 7/9 : Enrichissement Unpaywall...")
            progress_bar_rennes.progress(70)
            result_df_rennes = enrich_w_upw_parallel(result_df_rennes.copy())
            st.success(f"Enrichissement Unpaywall pour {collection_a_chercher_rennes} terminé.")

        # --- Étape 8 : Permissions de dépôt ---
        with st.spinner(f"Récupération des permissions pour {collection_a_chercher_rennes}..."):
            progress_text_area_rennes.info("Étape 8/9 : Récupération des permissions de dépôt...")
            progress_bar_rennes.progress(80)
            result_df_rennes = add_permissions_parallel(result_df_rennes.copy())
            st.success(f"Permissions pour {collection_a_chercher_rennes} récupérées.")

        # --- Étape 9 : Déduction des actions et auteurs ---
        progress_text_area_rennes.info("Étape 9/9 : Déduction des actions et traitement des auteurs...")
        if 'Action' not in result_df_rennes.columns: result_df_rennes['Action'] = pd.NA
        result_df_rennes['Action'] = result_df_rennes.apply(deduce_todo, axis=1)

        if fetch_authors_rennes: 
            with st.spinner(f"Récupération des auteurs Crossref pour {collection_a_chercher_rennes}..."):
                if 'doi' in result_df_rennes.columns:
                    from concurrent.futures import ThreadPoolExecutor 
                    from tqdm import tqdm 

                    dois_for_authors_rennes = result_df_rennes['doi'].fillna("").tolist()
                    authors_results_rennes = []
                    with ThreadPoolExecutor(max_workers=10) as executor:
                        authors_results_rennes = list(tqdm(executor.map(get_authors_from_crossref, dois_for_authors_rennes), total=len(dois_for_authors_rennes), desc="Auteurs Crossref (rennes)"))
                    
                    result_df_rennes['Auteurs_Crossref'] = ['; '.join(author_l) if isinstance(author_l, list) and not any("Erreur" in str(a) or "Timeout" in str(a) for a in author_l) else (author_l[0] if isinstance(author_l, list) and author_l else '') for author_l in authors_results_rennes]
                    st.success(f"Auteurs Crossref pour {collection_a_chercher_rennes} récupérés.")
                else:
                    st.warning("Colonne 'doi' non trouvée, impossible de récupérer les auteurs pour la version rennes.")
                    result_df_rennes['Auteurs_Crossref'] = ''
            
            if compare_authors_rennes and uploaded_authors_file_rennes:
                with st.spinner(f"Comparaison des auteurs (fichier) pour {collection_a_chercher_rennes}..."):
                    try:
                        user_authors_df_rennes_file = pd.read_csv(uploaded_authors_file_rennes)
                        if not ({'collection', user_authors_df_rennes_file.columns[1]} <= set(user_authors_df_rennes_file.columns)):
                            st.error("Fichier CSV auteurs mal formaté pour la version rennes.")
                        else:
                            author_name_col_rennes_file = user_authors_df_rennes_file.columns[1]
                            noms_ref_rennes_list = user_authors_df_rennes_file[user_authors_df_rennes_file["collection"].astype(str).str.lower() == str(collection_a_chercher_rennes).lower()][author_name_col_rennes_file].dropna().unique().tolist()
                            if not noms_ref_rennes_list:
                                st.warning(f"Aucun chercheur pour '{collection_a_chercher_rennes}' dans le fichier fourni (rennes).")
                            else:
                                chercheur_map_rennes_file = {normalize_name(n): n for n in noms_ref_rennes_list}
                                initial_map_rennes_file = {get_initial_form(normalize_name(n)): n for n in noms_ref_rennes_list}
                                from difflib import get_close_matches 

                                def detect_known_authors_rennes_file(authors_str_rennes):
                                    if pd.isna(authors_str_rennes) or not str(authors_str_rennes).strip() or "Erreur" in authors_str_rennes or "Timeout" in authors_str_rennes: return ""
                                    authors_pub_rennes = [a.strip() for a in str(authors_str_rennes).split(';') if a.strip()]
                                    detectes_originaux_rennes = set()
                                    for author_o_rennes in authors_pub_rennes:
                                        author_n_rennes = normalize_name(author_o_rennes)
                                        author_i_n_rennes = get_initial_form(author_n_rennes)
                                        match_c_rennes = get_close_matches(author_n_rennes, chercheur_map_rennes_file.keys(), n=1, cutoff=0.85)
                                        if match_c_rennes:
                                            detectes_originaux_rennes.add(chercheur_map_rennes_file[match_c_rennes[0]])
                                            continue
                                        match_i_rennes = get_close_matches(author_i_n_rennes, initial_map_rennes_file.keys(), n=1, cutoff=0.9)
                                        if match_i_rennes:
                                            detectes_originaux_rennes.add(initial_map_rennes_file[match_i_rennes[0]])
                                    return "; ".join(sorted(list(detectes_originaux_rennes))) if detectes_originaux_rennes else ""
                                result_df_rennes['Auteurs_Laboratoire_Détectés'] = result_df_rennes['Auteurs_Crossref'].apply(detect_known_authors_rennes_file)
                                st.success(f"Comparaison auteurs (fichier) pour {collection_a_chercher_rennes} terminée.")
                    except Exception as e_auth_file_rennes_exc:
                        st.error(f"Erreur fichier auteurs (rennes): {e_auth_file_rennes_exc}")
            elif compare_authors_rennes and not uploaded_authors_file_rennes:
                 st.warning("Veuillez téléverser un fichier CSV de chercheurs pour la comparaison des auteurs (rennes).")

        progress_bar_rennes.progress(90)
        st.success(f"Déduction des actions et traitement des auteurs pour {collection_a_chercher_rennes} terminés.")
        
        st.dataframe(result_df_rennes)
        # --- Sauvegarde persistante des résultats pour permettre les actions après rerun ---
        try:
            st.session_state['last_result_df'] = result_df_rennes.to_dict(orient='records')
            st.session_state['last_collection'] = collection_a_chercher_rennes
            st.success("✅ Résultats sauvegardés en session — vous pouvez générer le ZIP sans relancer la recherche.")
        except Exception as e:
            st.warning(f"Impossible de sauvegarder les résultats en session: {e}")

        # --- Export XML HAL (préparation) ---
        st.write("Aperçu (head) des résultats :", result_df_rennes.head())
        st.write(f"Total lignes result_df_rennes : {len(result_df_rennes)}")

        # --- Export CSV classique ---
        if not result_df_rennes.empty:
            csv_export_rennes_data = result_df_rennes.to_csv(index=False, encoding='utf-8-sig')
            output_filename_rennes_final = f"c2LabHAL_resultats_{collection_a_chercher_rennes.replace(' ', '_')}_{start_year_rennes}-{end_year_rennes}.csv"
            st.download_button(
                label=f"📥 Télécharger les résultats pour {collection_a_chercher_rennes}",
                data=csv_export_rennes_data,
                file_name=output_filename_rennes_final,
                mime="text/csv",
                key=f"download_rennes_{collection_a_chercher_rennes}"
            )

        # -----------------------
        # Fonctions utilitaires pour assainir les auteurs/institutions
        # -----------------------

        def _ensure_authors_struct(auth_field):
            """
            Transforme n'importe quelle forme de 'authors' en liste normalisée :
            [{'name':..., 'orcid':..., 'raw_affiliations':[...]}]
            """
            import json

            # Cas vide
            if not auth_field:
                return []

            # Si c'est déjà une liste de dicts
            if isinstance(auth_field, list):
                clean_list = []
                for a in auth_field:
                    if isinstance(a, dict):
                        clean_list.append({
                            "name": _safe_text(a.get("name", "")),
                            "orcid": _safe_text(a.get("orcid", "")),
                            "raw_affiliations": _ensure_list(a.get("raw_affiliations"))
                        })
                    elif isinstance(a, str):
                        # cas étrange : liste de strings
                        clean_list.append({"name": _safe_text(a), "orcid": "", "raw_affiliations": []})
                return clean_list

            # Si c'est une chaîne JSON ou une string quelconque
            if isinstance(auth_field, str):
                try:
                    parsed = json.loads(auth_field)
                    return _ensure_authors_struct(parsed)
                except Exception:
                    # plain string (ex: "John Doe, Jane Smith")
                    parts = [p.strip() for p in auth_field.split(",") if p.strip()]
                    return [{"name": p, "orcid": "", "raw_affiliations": []} for p in parts]

            # Sinon : cas inattendu (dict unique, None, etc.)
            if isinstance(auth_field, dict):
                return [_ensure_authors_struct(auth_field)]
            return []

        def _ensure_institutions_struct(inst_field):
            """
            Retourne une liste de dicts d'institutions {'display_name','ror','type','country'}.
            Gère list/dict/str.
            """
            if inst_field is None:
                return []
            if isinstance(inst_field, list):
                cleaned = []
                for it in inst_field:
                    if isinstance(it, dict):
                        cleaned.append({
                            "display_name": _safe_text(it.get("display_name", "")).strip(),
                            "ror": _safe_text(it.get("ror", "")).strip(),
                            "type": it.get("type", "institution"),
                            "country": it.get("country", "")
                        })
                    elif isinstance(it, str):
                        cleaned.append({"display_name": it.strip(), "ror": "", "type": "institution", "country": ""})
                return cleaned
            if isinstance(inst_field, dict):
                return [_ensure_institutions_struct([inst_field])[0]]
            # str
            s = str(inst_field).strip()
            if not s:
                return []
            # si c'est 'display_name|ror' ou 'ror|display_name' possible split
            if '|' in s:
                parts = [p.strip() for p in s.split('|')]
                return [{"display_name": parts[0], "ror": parts[1] if len(parts)>1 else "", "type": "institution", "country": ""}]
            return [{"display_name": s, "ror": "", "type": "institution", "country": ""}]

        # -----------------------
        # Panneau minimal : un seul bouton visible "Télécharger le ZIP"
        # -----------------------
        if st.session_state.get('last_result_df_filtered') is not None:
            last_df = pd.DataFrame(st.session_state['last_result_df_filtered'])
            last_collection = st.session_state.get('last_collection', 'unknown')
        
            st.markdown("---")
            st.subheader(f"📦 Export XML HAL — collection : {last_collection}")
            st.write(f"Résultats filtrés : {len(last_df)} publications prêtes pour export (hors HAL ou hors collection)")
        
            pubs_to_export = last_df.to_dict(orient="records")
        
            st.info(f"Publications candidates : {len(pubs_to_export)}")
            if pubs_to_export:
                st.write(pd.DataFrame(pubs_to_export[:3]))
        
            # Normalisation stricte des structures avant génération
            for pub in pubs_to_export:
                pub["authors"] = _ensure_authors_struct(pub.get("authors", []))
                pub["institutions"] = _ensure_institutions_struct(pub.get("institutions", []))
        
            # Bouton unique — génère le ZIP
            if st.button(f"⬇️ Télécharger le fichier ZIP des XML HAL ({len(pubs_to_export)})", key=f"dlzip_{last_collection}"):
        
                try:
                    with st.spinner("Génération du ZIP en cours..."):
                        zipbuf = generate_zip_from_xmls(pubs_to_export)
        
                        # ✅ Normalisation en bytes
                        zip_bytes = None
                        if zipbuf:
                            if hasattr(zipbuf, "getvalue"):
                                zip_bytes = zipbuf.getvalue()
                            elif isinstance(zipbuf, (bytes, bytearray)):
                                zip_bytes = bytes(zipbuf)
                            else:
                                try:
                                    zipbuf.seek(0)
                                    zip_bytes = zipbuf.read()
                                except Exception:
                                    zip_bytes = None
        
                        if zip_bytes:
                            st.session_state['zip_buffer'] = zip_bytes
                            st.success("✅ ZIP prêt — cliquez sur le bouton ci-dessous pour télécharger.")
                            st.write(f"DEBUG : taille ZIP (octets) = {len(zip_bytes)}")
                        else:
                            st.error("Erreur : la génération du ZIP a renvoyé None ou un objet vide.")
                except Exception as e:
                    import traceback
                    st.error(f"Erreur pendant la génération du ZIP : {e}")
                    st.text(traceback.format_exc())
        
            # Bouton de téléchargement final
            if st.session_state.get('zip_buffer'):
                st.download_button(
                    label="⬇️ Télécharger le fichier ZIP des XML HAL (cliquer ici)",
                    data=st.session_state['zip_buffer'],
                    file_name=f"hal_exports_{last_collection}.zip",
                    mime="application/zip",
                    key=f"download_zip_{last_collection}"
                )
        
        else:
            st.info("⚠️ Aucune recherche en session ou données filtrées absentes. Lancez d'abord la recherche.")

        progress_bar_rennes.progress(100)
        progress_text_area_rennes.success(f"🎉 Traitement pour {collection_a_chercher_rennes} terminé avec succès !")



# -----------------------
# Fonctions utilitaires pour assainir les auteurs/institutions
# -----------------------

def _ensure_authors_struct(auth_field):
    """
    Transforme n'importe quelle forme de 'authors' en liste normalisée :
    [{'name':..., 'orcid':..., 'raw_affiliations':[...]}]
    """
    import json

    # Cas vide
    if not auth_field:
        return []

    # Si c'est déjà une liste de dicts
    if isinstance(auth_field, list):
        clean_list = []
        for a in auth_field:
            if isinstance(a, dict):
                clean_list.append({
                    "name": _safe_text(a.get("name", "")),
                    "orcid": _safe_text(a.get("orcid", "")),
                    "raw_affiliations": _ensure_list(a.get("raw_affiliations"))
                })
            elif isinstance(a, str):
                # cas étrange : liste de strings
                clean_list.append({"name": _safe_text(a), "orcid": "", "raw_affiliations": []})
        return clean_list

    # Si c'est une chaîne JSON ou une string quelconque
    if isinstance(auth_field, str):
        try:
            parsed = json.loads(auth_field)
            return _ensure_authors_struct(parsed)
        except Exception:
            # plain string (ex: "John Doe, Jane Smith")
            parts = [p.strip() for p in auth_field.split(",") if p.strip()]
            return [{"name": p, "orcid": "", "raw_affiliations": []} for p in parts]

    # Sinon : cas inattendu (dict unique, None, etc.)
    if isinstance(auth_field, dict):
        return [_ensure_authors_struct(auth_field)]
    return []

def _ensure_institutions_struct(inst_field):
    """
    Retourne une liste de dicts d'institutions {'display_name','ror','type','country'}.
    Gère list/dict/str.
    """
    if inst_field is None:
        return []
    if isinstance(inst_field, list):
        cleaned = []
        for it in inst_field:
            if isinstance(it, dict):
                cleaned.append({
                    "display_name": _safe_text(it.get("display_name", "")).strip(),
                    "ror": _safe_text(it.get("ror", "")).strip(),
                    "type": it.get("type", "institution"),
                    "country": it.get("country", "")
                })
            elif isinstance(it, str):
                cleaned.append({"display_name": it.strip(), "ror": "", "type": "institution", "country": ""})
        return cleaned
    if isinstance(inst_field, dict):
        return [_ensure_institutions_struct([inst_field])[0]]
    # str
    s = str(inst_field).strip()
    if not s:
        return []
    # si c'est 'display_name|ror' ou 'ror|display_name' possible split
    if '|' in s:
        parts = [p.strip() for p in s.split('|')]
        return [{"display_name": parts[0], "ror": parts[1] if len(parts)>1 else "", "type": "institution", "country": ""}]
    return [{"display_name": s, "ror": "", "type": "institution", "country": ""}]

if __name__ == "__main__":
    main()
