import io
import re
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
import traceback
import json
import ast

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

def extract_authors_from_openalex_json(openalex_input):
    """
    Accepts:
      - a dict representing an OpenAlex work (will try work['authorships'])
      - OR a list of authorship dicts
      - OR a JSON string representing either of the above
    Returns a list of authors in the form:
      [{"name": ..., "orcid": ..., "raw_affiliations": [...]}, ...]
    """
    authors_list = []

    # 1) Normalize to a python object (list/dict)
    auths = None
    try:
        # If it's a dict representing the work, try to extract 'authorships'
        if isinstance(openalex_input, dict):
            auths = openalex_input.get('authorships') or openalex_input.get('authorships_parsed') or None
            # some exports may already have 'authorships' flattened under a different key
        elif isinstance(openalex_input, list):
            auths = openalex_input
        elif isinstance(openalex_input, str):
            # try parse JSON string
            try:
                parsed = json.loads(openalex_input)
                if isinstance(parsed, dict):
                    auths = parsed.get('authorships') or parsed
                else:
                    auths = parsed
            except Exception:
                # it's a plain string => nothing to do
                auths = None
        else:
            auths = None
    except Exception as e:
        st.warning(f"extract_authors_from_openalex_json: erreur normalisation input: {e}")
        auths = None

    if not auths:
        return []  # nothing to extract

    # 2) Iterate safely over auths and extract fields
    for idx, auth in enumerate(auths):
        try:
            # If element is a string, try to parse
            if isinstance(auth, str):
                try:
                    auth = json.loads(auth)
                except Exception:
                    # can't parse string -> skip
                    continue

            if not isinstance(auth, dict):
                continue

            # raw author name (OpenAlex uses 'raw_affiliation' for affiliation lines,
            # but raw author name is 'raw_author_name' per earlier convention)
            raw_name = auth.get('raw_author_name') or auth.get('author', {}).get('display_name') or auth.get('author_raw', '') or ""

            # ORCID: sometimes under auth['author']['orcid'] (full url or path)
            orcid = ""
            try:
                orcid = auth.get('author', {}).get('orcid') or auth.get('author_orcid') or ""
                # normalize minimal: if it's e.g. '0000-000...' prefix with https if needed
                if orcid and not orcid.startswith("http"):
                    if orcid.count("-") == 3:
                        orcid = "https://orcid.org/" + orcid
            except Exception:
                orcid = ""

            # Raw affiliations: OpenAlex 'institutions' is a list of dicts; use their 'display_name'
            raw_affs = []
            insts = auth.get('institutions') or auth.get('affiliations') or []
            if isinstance(insts, list):
                for i in insts:
                    if isinstance(i, dict):
                        dn = i.get('display_name') or i.get('raw_affiliation') or ""
                        if dn:
                            raw_affs.append(dn)
                    elif isinstance(i, str) and i.strip():
                        raw_affs.append(i.strip())

            # Fallback: some records include a 'raw_affiliation_strings' or similar
            if not raw_affs:
                # try other fields
                possible_raws = auth.get('raw_affiliation_strings') or auth.get('raw_affiliations') or []
                if isinstance(possible_raws, list):
                    for r in possible_raws:
                        if isinstance(r, str) and r.strip():
                            raw_affs.append(r.strip())

            authors_list.append({
                "name": _safe_text(raw_name),
                "orcid": _safe_text(orcid),
                "raw_affiliations": [ _safe_text(r) for r in raw_affs ]
            })
        except Exception as e:
            st.warning(f"Erreur dans extract_authors_from_openalex_json pour un authorship (idx {idx}): {e}")
            continue

    return authors_list

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
    """Génère un zip à partir de publications_list (liste de dicts).
       Affiche du debug dans Streamlit pour chaque item.
    """
    st.info(f"🔧 generate_zip_from_xmls démarrée — {len(publications_list)} publications à traiter")
    zip_buffer = io.BytesIO()
    written_files = []  # debug: keep track of filenames actually written

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for idx, pub in enumerate(publications_list):
            try:
                # debug: afficher statut source/Statut_HAL/Action
                title_preview = (pub.get('Title') or pub.get('title') or "")[:120]
                doi_preview = pub.get('doi') or ""
                statut_hal = pub.get('Statut_HAL') or pub.get('statut_hal') or ""
                action_val = pub.get('Action') or pub.get('action') or ""

                st.write(f"  · [{idx+1}/{len(publications_list)}] Titre: {title_preview}")
                st.write(f"      DOI: {doi_preview} | Statut_HAL: {statut_hal} | Action: {action_val}")

                # Génération XML
                xml_bytes = generate_hal_xml(pub)  # ta fonction
                if not xml_bytes:
                    st.warning(f"    ? generate_hal_xml a retourné None pour la pub {idx+1} ({title_preview})")
                    continue

                # filename sûr
                filename = f"{_safe_filename(pub.get('Title','untitled'))}.xml"
                zip_file.writestr(filename, xml_bytes)
                written_files.append({"filename": filename, "title": title_preview, "doi": doi_preview, "statut_hal": statut_hal, "action": action_val})

            except Exception as e:
                st.error(f"Erreur lors de la génération XML pour index {idx} : {e}")
                st.text(traceback.format_exc())
                continue

    zip_buffer.seek(0)
    zip_bytes = zip_buffer.getvalue()

    st.info("✅ generate_zip_from_xmls terminée")
    st.write("🗂 Fichiers écrits dans le ZIP :")
    for w in written_files:
        st.write(f" - {w['filename']} | DOI: {w['doi']} | Statut_HAL: {w['statut_hal']} | Action: {w.get('action','')}")

    return zip_bytes

    
