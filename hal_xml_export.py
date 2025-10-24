import io
import re
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
import traceback

# ==============================
# Utilitaires internes
# ==============================

def _safe_text(value):
    """Retourne une chaîne sûre, encodable en UTF-8 et utilisable dans un XML (jamais None ni NaN)."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    try:
        return str(value)
    except Exception:
        return ""

def _safe_filename(s, maxlen=60):
    """Nettoie le nom du fichier pour le ZIP."""
    s2 = _safe_text(s)
    s2 = re.sub(r'[\\/:"*?<>|]+', '_', s2)
    s2 = s2.strip()
    if len(s2) > maxlen:
        s2 = s2[:maxlen].rstrip()
    if not s2:
        s2 = "untitled"
    return s2
    
def _ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def _build_listorg_from_institutions(institutions):
    """
    institutions: liste de dicts attendus, chaque dict peut contenir:
      - 'ror' (ex: 'https://ror.org/05qec5a53' ou '05qec5a53')
      - 'display_name' (nom de l'institution)
      - 'type' (optionnel: 'institution'/'researchteam' etc)
      - 'country' (optionnel)
    Retourne un Element <back><listOrg>...</listOrg></back>
    """
    if not institutions:
        return None

    back_el = ET.Element("back")
    listOrg = ET.SubElement(back_el, "listOrg", {"type": "structures"})
    for idx, inst in enumerate(institutions):
        # id attribute unique local
        xml_id = f"localStruct-Aff{idx+1}"
        org_type = inst.get("type", "institution")
        org_el = ET.SubElement(listOrg, "org", {"type": org_type, "xml:id": xml_id})
        # ROR normalisé
        ror = _safe_text(inst.get("ror", ""))
        if ror:
            # si l'utilisateur a mis juste l'id, on le transforme en URL ror.org/
            if ror.startswith("http"):
                idno_val = ror
            else:
                idno_val = f"https://ror.org/{ror}"
            ET.SubElement(org_el, "idno", {"type": "ROR"}).text = _safe_text(idno_val)
        # orgName
        ET.SubElement(org_el, "orgName").text = _safe_text(inst.get("display_name", ""))
        # desc/address minimal si pays fourni
        country = _safe_text(inst.get("country", ""))
        if country:
            desc = ET.SubElement(org_el, "desc")
            addr = ET.SubElement(desc, "address")
            ET.SubElement(addr, "country", {"key": country})
    return back_el, listOrg  # on renvoie aussi listOrg si utile

# ==============================
# Extraction des auteurs OpenAlex
# ==============================

def extract_authors_from_openalex_json(openalex_authorships):
    """
    Extrait la liste des auteurs depuis la structure OpenAlex JSON (champ 'authorships').
    Chaque auteur contient name, orcid et raw affiliations.
    """
    authors_data = []

    if not openalex_authorships:
        return authors_data

    for auth in openalex_authorships:
        try:
            author_entry = {
                "name": _safe_text(auth.get("raw_author_name", "")),
                "orcid": _safe_text(auth.get("author", {}).get("orcid", "")),
                "raw_affiliations": []
            }

            for aff in auth.get("institutions", []):
                raw_aff_text = aff.get("display_name", "")
                if raw_aff_text:
                    author_entry["raw_affiliations"].append(raw_aff_text)

            authors_data.append(author_entry)
        except Exception as e:
            st.warning(f"Erreur dans extract_authors_from_openalex_json : {e}")
            continue

    return authors_data


# ==============================
# Génération XML HAL (un par publication)
# ==============================

def generate_hal_xml(pub_data):
    """
    Génère un TEI/HAL XML (bytes) pour la publication fournie.
    pub_data doit être un dict contenant idéalement :
      - 'Title' (str)
      - 'doi' (str) (optionnel)
      - 'pubmed' (str) (optionnel)
      - 'Source title' (journal title)
      - 'Date' (année ou date)
      - 'publisher' (optionnel)
      - 'authors' : liste de dicts avec champs possibles:
           - 'name' (utiliser raw_author_name si venant d'OpenAlex)
           - 'orcid' (url ORCID)
           - 'raw_affiliations' : liste de lignes textes (raw_aff)
      - 'institutions' : liste de dicts (ror, display_name, type, country) pour listOrg
      - 'keywords' : liste de mots-clés (optionnel)
      - 'abstract' : texte (optionnel)
    La fonction est tolérante si certaines clés manquent.
    """

    # racine TEI
    TEI = ET.Element(
        "TEI",
        {
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns": "http://www.tei-c.org/ns/1.0",
            "xmlns:hal": "http://hal.archives-ouvertes.fr/",
            "xsi:schemaLocation": "http://www.tei-c.org/ns/1.0 http://api.archives-ouvertes.fr/documents/aofr-sword.xsd",
        },
    )

    text = ET.SubElement(TEI, "text")
    body = ET.SubElement(text, "body")
    listBibl = ET.SubElement(body, "listBibl")
    biblFull = ET.SubElement(listBibl, "biblFull")

    # --- titleStmt / seriesStmt / notesStmt minimal ---
    titleStmt = ET.SubElement(biblFull, "titleStmt")
    seriesStmt = ET.SubElement(biblFull, "seriesStmt")
    notesStmt = ET.SubElement(biblFull, "notesStmt")
    # exemples de notes (peer/audience/popular) minimal ; tu peux enrichir selon besoin
    ET.SubElement(notesStmt, "note", {"type": "audience", "n": "2"})
    ET.SubElement(notesStmt, "note", {"type": "popular", "n": "0"}).text = "No"
    ET.SubElement(notesStmt, "note", {"type": "peer", "n": "1"}).text = "Yes"

    # --- sourceDesc / biblStruct ---
    sourceDesc = ET.SubElement(biblFull, "sourceDesc")
    biblStruct = ET.SubElement(sourceDesc, "biblStruct")

    # analytic (titre + auteurs)
    analytic = ET.SubElement(biblStruct, "analytic")
    ET.SubElement(analytic, "title", {"xml:lang": "en"}).text = _safe_text(pub_data.get("Title", ""))

    # auteurs : on attend une liste de dicts {'name','orcid','raw_affiliations'}
    authors = _ensure_list(pub_data.get("authors", []))
    for author in authors:
        author_el = ET.SubElement(analytic, "author", {"role": "aut"})
        persName = ET.SubElement(author_el, "persName")
        raw_name = _safe_text(author.get("name", ""))
        # split prénom / nom (conservateur : split first space)
        name_parts = raw_name.split(" ", 1) if raw_name else []
        if len(name_parts) == 2:
            ET.SubElement(persName, "forename", {"type": "first"}).text = _safe_text(name_parts[0])
            ET.SubElement(persName, "surname").text = _safe_text(name_parts[1])
        elif len(name_parts) == 1:
            ET.SubElement(persName, "surname").text = _safe_text(name_parts[0])
        else:
            ET.SubElement(persName, "surname").text = ""

        # ORCID si présent (mettre la forme https://orcid.org/0000-...)
        if author.get("orcid"):
            ET.SubElement(author_el, "idno", {"type": "ORCID"}).text = _safe_text(author.get("orcid"))

        # rawAffs répétables
        for raw_aff in _ensure_list(author.get("raw_affiliations", [])):
            ET.SubElement(author_el, "rawAffs").text = _safe_text(raw_aff)

        # affiliation ref: on ne met pas automatiquement ref="#localStruct-AffX" ici
        # (on peut ajouter plus tard une logique pour relier auteurs->orgs)

    # monogr (journal) / imprint
    monogr = ET.SubElement(biblStruct, "monogr")
    # possibilité d'inclure ISSN/eISSN si fournis (pub_data['issn'], 'eissn'...)
    if pub_data.get("issn"):
        ET.SubElement(monogr, "idno", {"type": "issn"}).text = _safe_text(pub_data.get("issn"))
    if pub_data.get("eissn"):
        ET.SubElement(monogr, "idno", {"type": "eissn"}).text = _safe_text(pub_data.get("eissn"))

    ET.SubElement(monogr, "title", {"level": "j"}).text = _safe_text(pub_data.get("Source title", ""))
    imprint = ET.SubElement(monogr, "imprint")
    ET.SubElement(imprint, "publisher").text = _safe_text(pub_data.get("publisher", ""))
    ET.SubElement(imprint, "biblScope", {"unit": "volume"}).text = _safe_text(pub_data.get("volume", ""))
    ET.SubElement(imprint, "biblScope", {"unit": "issue"}).text = _safe_text(pub_data.get("issue", ""))
    ET.SubElement(imprint, "biblScope", {"unit": "pp"}).text = _safe_text(pub_data.get("pages", ""))
    ET.SubElement(imprint, "date", {"type": "datePub"}).text = _safe_text(pub_data.get("Date", ""))

    # identifiants (doi / pubmed)
    if pub_data.get("doi"):
        ET.SubElement(biblStruct, "idno", {"type": "doi"}).text = _safe_text(pub_data.get("doi"))
    if pub_data.get("pubmed"):
        ET.SubElement(biblStruct, "idno", {"type": "pubmed"}).text = _safe_text(pub_data.get("pubmed"))

    # profileDesc (langUsage, keywords, classCode, abstract)
    profileDesc = ET.SubElement(biblFull, "profileDesc")
    langUsage = ET.SubElement(profileDesc, "langUsage")
    ET.SubElement(langUsage, "language", {"ident": "en"}).text = _safe_text(pub_data.get("language", "English"))

    textClass = ET.SubElement(profileDesc, "textClass")
    keywords = _ensure_list(pub_data.get("keywords", []))
    if keywords:
        kw_el = ET.SubElement(textClass, "keywords", {"scheme": "author"})
        for kw in keywords:
            ET.SubElement(kw_el, "term", {"xml:lang": "en"}).text = _safe_text(kw)

    # halTypology / classCode (optionnel)
    class_code = _safe_text(pub_data.get("classCode", "ART"))
    ET.SubElement(textClass, "classCode", {"scheme": "halTypology", "n": class_code}).text = "Journal articles"

    # abstract si présent
    if pub_data.get("abstract"):
        ET.SubElement(profileDesc, "abstract", {"xml:lang": "en"}).text = _safe_text(pub_data.get("abstract"))

    # --- back / listOrg basé sur institutions fournies dans pub_data ---
    institutions = _ensure_list(pub_data.get("institutions", []))
    if institutions:
        back_el, listOrg_el = _build_listorg_from_institutions(institutions)
        if back_el is not None:
            # attacher back à text
            text.append(back_el)

    # Produire bytes XML (avec déclaration)
    xml_bytes = ET.tostring(TEI, encoding="utf-8", xml_declaration=True)
    return xml_bytes


# ==============================
# Génération du ZIP global
# ==============================

def generate_zip_from_xmls(publications_list):
    """
    Génère un fichier ZIP en mémoire contenant un fichier XML par publication.
    Retourne un objet BytesIO prêt à être téléchargé.
    """
    st.info(f"🔧 generate_zip_from_xmls démarrée — {len(publications_list)} publications")
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for idx, pub in enumerate(publications_list):
            try:
                title_preview = pub.get('Title', '') if isinstance(pub, dict) else str(pub)
                st.write(f"  · Traitement [{idx+1}/{len(publications_list)}] : {title_preview[:80]}")
                xml_bytes = generate_hal_xml(pub)  # ma fonction existante
                if not xml_bytes:
                    st.warning(f"    → generate_hal_xml a retourné None pour la pub {idx+1}")
                    continue
                filename = f"{_safe_filename(pub.get('Title','untitled'))}.xml"
                zip_file.writestr(filename, xml_bytes)
            except Exception as e:
                st.error(f"Erreur lors de la génération XML pour index {idx} : {e}")
                st.text(traceback.format_exc())
                # On continue sur les autres publications
                continue
    zip_buffer.seek(0)
    st.info("✅ generate_zip_from_xmls terminée")
    return zip_buffer
    
