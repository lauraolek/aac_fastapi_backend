from estnltk import Text
from estnltk.vabamorf.morf import synthesize
from typing import List

def teisenda_ma_tahan_lauseosa(sisend_loend: List[str]) -> List[str]:
    """
    Käänab ja pöörab loendi sõnu vastavalt konstruktsiooni reeglitele, mis järgnevad
    käivitusfraasile (nt "Ma tahan"). Konjugatsioon rakendub ainult pärast fraasi.
    Kasutab uuemat EstNLTK kättesaamise süntaksit.
    """
    
    # Määrame võimalikud käivitusfraasid (sulgudes on näited tulevikuks, et funktsioon oleks üldistatav)
    TRIGGER_PHRASES = ["ma tahan"]
    
    # 1. Leiame esimese käivitusfraasi indeksi
    trigger_index = -1
    
    # Otsime loendist esimest sõna, mis vastab mõnele käivitusfraasile (case-insensitive)
    for i, word in enumerate(sisend_loend):
        if word.lower() in TRIGGER_PHRASES:
            trigger_index = i
            break
            
    if trigger_index == -1:
        # Kui ühtegi käivitusfraasi pole leitud, tagastame loendi muutmata kujul.
        return sisend_loend

    # Sõnad enne käivitusfraasi (muutumatu osa) ja fraas ise.
    # Need sõnad lisatakse väljundisse muutmata kujul.
    valjund_loend = sisend_loend[:trigger_index + 1]
    
    # Sõnad, mida on vaja töödelda (sõnad pärast käivitusfraasi)
    sonad_tootlemiseks = sisend_loend[trigger_index + 1:]
    
    for sona in sonad_tootlemiseks:
        # Püüame kinni tühjad sõnad või korduvad käivitusfraasid
        if not sona or sona.isspace() or sona.lower() in TRIGGER_PHRASES:
            valjund_loend.append(sona)
            continue

        try:
            # 1. Analüüs: Leiame sõnaliigi ja algvormi (lemma)
            text_obj = Text(sona)
            # Kasutame morfoloogiliseks analüüsiks (tag) kihti
            text_obj.tag_layer(['morph_analysis'])

            if not text_obj['morph_analysis'] or not text_obj['morph_analysis'][0].annotations:
                # Kui analüüs ebaõnnestus (nt tundmatu lühend), jätame sõna muutmata
                valjund_loend.append(sona)
                continue

            # Võtame esimese (EstNLTK poolt parimaks peetud) analüüsi
            analyys = text_obj['morph_analysis'][0].annotations[0]
            lemma = analyys.get('lemma')
            sonaliik = analyys.get('partofspeech') # Nt S, A, V, Num

            # Määra soovitud tunnused sõnaliigi järgi
            soovitud_tunnus = None

            if sonaliik in ['S', 'A', 'N', 'Num', "P"]: # Nimisõna, Omadussõna, Arvsõna, Asesõna
                # Määrame ainsuse partitiivi (sg p)
                soovitud_tunnus = "sg p" 
            elif sonaliik == 'V': # Tegusõna
                # Määrame da-infinitiivi (da)
                soovitud_tunnus = "da" 
            
            # Kui soovitud tunnus on määratud, proovime sünteesida
            if soovitud_tunnus:
                # 2. Generatsioon: Moodustame uue vormi algvormist ja tunnustest
                try:
                    # Synthesize tagastab loendi võimalikest vormidest, võtame esimese
                    genereeritud_vormid = synthesize(lemma, soovitud_tunnus)

                    if genereeritud_vormid:
                        valjund_loend.append(genereeritud_vormid[-1])
                    else:
                        # Kui generatsioon ebaõnnestus, kasutame algvormi
                        valjund_loend.append(lemma)
                except Exception:
                    # Jätame sõna muutmata, kui generatsioon viskab vea
                    valjund_loend.append(sona)
            else:
                # Jätame muud sõnad (sidesõnad, määrsõnad jne.) muutmata
                valjund_loend.append(sona)

        except Exception:
             # Üldine veapüük analüüsi või Text loomise ajal
             valjund_loend.append(sona)

    return valjund_loend
