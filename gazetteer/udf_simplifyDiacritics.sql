CREATE OR REPLACE FUNCTION `localityservice.functions.simplifyDiacritics`(str STRING) RETURNS STRING LANGUAGE js AS R"""
{
// Normalizes unicode, lowercases, and changes diacritics to ASCII "equivalents"
var defaultDiacriticsRemovalMap = [ 
{'base':'A', 'letters':/[AⒶＡÀÁ ẦẤẪẨÃĀĂẰẮẴẲȦǠÄǞẢÅǺǍȀȂẠẬẶḀĄȺⱯ]/g}, 
{'base':'AA','letters':/[Ꜳ]/g}, 
{'base':'AE','letters':/[ÆǼǢ]/g}, 
{'base':'AO','letters':/[Ꜵ]/g}, 
{'base':'AU','letters':/[Ꜷ]/g}, 
{'base':'AV','letters':/[ꜸꜺ]/g}, 
{'base':'AY','letters':/[Ꜽ]/g}, 
{'base':'B', 'letters':/[BⒷＢḂḄḆɃƂƁ]/g}, 
{'base':'C', 'letters':/[CⒸＣĆĈĊČÇḈƇȻꜾ]/g}, 
{'base':'D', 'letters':/[DⒹＤḊĎḌḐḒḎĐƋƊƉꝹ]/g}, 
{'base':'DZ','letters':/[ǱǄ]/g}, 
{'base':'Dz','letters':/[ǲǅ]/g}, 
{'base':'E', 'letters':/[EⒺＥÈÉÊỀẾỄỂẼĒḔḖĔĖËẺĚȄȆẸỆȨḜĘḘḚƐƎ]/g}, 
{'base':'F', 'letters':/[FⒻＦḞƑꝻ]/g}, 
{'base':'G', 'letters':/[GⒼＧǴĜḠĞĠǦĢǤƓꞠꝽꝾ]/g}, 
{'base':'H', 'letters':/[HⒽＨĤḢḦȞḤḨḪĦⱧⱵꞍ]/g}, 
{'base':'I', 'letters':/[IⒾＩÌÍÎĨĪĬİÏḮỈǏȈȊỊĮḬƗ]/g}, 
{'base':'J', 'letters':/[JⒿＪĴɈ]/g}, 
{'base':'K', 'letters':/[KⓀＫḰǨḲĶḴƘⱩꝀꝂꝄꞢ]/g}, 
{'base':'L', 'letters':/[LⓁＬĿĹĽḶḸĻḼḺŁȽⱢⱠꝈꝆꞀ]/g}, 
{'base':'LJ','letters':/[Ǉ]/g}, 
{'base':'Lj','letters':/[ǈ]/g}, 
{'base':'M', 'letters':/[MⓂＭḾṀṂⱮƜ]/g}, 
{'base':'N', 'letters':/[NⓃＮǸŃÑṄŇṆŅṊṈȠƝꞐꞤ]/g}, 
{'base':'NJ','letters':/[Ǌ]/g}, 
{'base':'Nj','letters':/[ǋ]/g}, 
{'base':'O', 'letters':/[OⓄＯÒÓÔỒỐỖỔÕṌȬṎŌṐṒŎȮȰÖȪỎŐǑȌȎƠỜỚỠỞỢỌỘǪǬØǾƆƟꝊꝌ]/g}, 
{'base':'OI','letters':/[Ƣ]/g}, 
{'base':'OO','letters':/[Ꝏ]/g}, 
{'base':'OU','letters':/[Ȣ]/g}, 
{'base':'P', 'letters':/[PⓅＰṔṖƤⱣꝐꝒꝔ]/g}, 
{'base':'Q', 'letters':/[QⓆＱꝖꝘɊ]/g}, 
{'base':'R', 'letters':/[RⓇＲŔṘŘȐȒṚṜŖṞɌⱤꝚꞦꞂ]/g}, 
{'base':'S', 'letters':/[SⓈＳẞŚṤŜṠŠṦṢṨȘŞⱾꞨꞄ]/g}, 
{'base':'SS', 'letters':/[ẞ]/g}, 
{'base':'T', 'letters':/[TⓉＴṪŤṬȚŢṰṮŦƬƮȾꞆ]/g}, 
{'base':'TZ','letters':/[Ꜩ]/g}, 
{'base':'U', 'letters':/[UⓊＵÙÚÛŨṸŪṺŬÜǛǗǕǙỦŮŰǓȔȖƯỪỨỮỬỰỤṲŲṶṴɄ]/g}, 
{'base':'V', 'letters':/[VⓋＶṼṾƲꝞɅ]/g}, 
{'base':'VY','letters':/[Ꝡ]/g}, 
{'base':'W', 'letters':/[WⓌＷẀẂŴẆẄẈⱲ]/g}, 
{'base':'X', 'letters':/[XⓍＸẊẌ]/g}, 
{'base':'Y', 'letters':/[YⓎＹỲÝŶỸȲẎŸỶỴƳɎỾ]/g}, 
{'base':'Z', 'letters':/[ZⓏＺŹẐŻŽẒẔƵȤⱿⱫꝢ]/g}, 
{'base':'a', 'letters':/[aⓐａẚàáâầấẫẩãāăằắẵẳȧǡäǟảåǻǎȁȃạậặḁąⱥɐ]/g}, 
{'base':'aa','letters':/[ꜳ]/g}, 
{'base':'ae','letters':/[æǽǣ]/g}, 
{'base':'ao','letters':/[ꜵ]/g}, 
{'base':'au','letters':/[ꜷ]/g}, 
{'base':'av','letters':/[ꜹꜻ]/g}, 
{'base':'ay','letters':/[ꜽ]/g}, 
{'base':'b', 'letters':/[bⓑｂḃḅḇƀƃɓ]/g}, 
{'base':'c', 'letters':/[cⓒｃćĉċčçḉƈȼꜿↄ]/g}, 
{'base':'d', 'letters':/[dⓓｄḋďḍḑḓḏđƌɖɗꝺ]/g}, 
{'base':'dz','letters':/[ǳǆ]/g}, 
{'base':'e', 'letters':/[eⓔｅèéêềếễểẽēḕḗĕėëẻěȅȇẹệȩḝęḙḛɇɛǝ]/g}, 
{'base':'f', 'letters':/[fⓕｆḟƒꝼ]/g}, 
{'base':'g', 'letters':/[gⓖｇǵĝḡğġǧģǥɠꞡᵹꝿ]/g}, 
{'base':'h', 'letters':/[hⓗｈĥḣḧȟḥḩḫẖħⱨⱶɥ]/g}, 
{'base':'hv','letters':/[ƕ]/g}, 
{'base':'i', 'letters':/[iⓘｉìíîĩīĭïḯỉǐȉȋịįḭɨı]/g}, 
{'base':'j', 'letters':/[jⓙｊĵǰɉ]/g}, 
{'base':'k', 'letters':/[kⓚｋḱǩḳķḵƙⱪꝁꝃꝅꞣ]/g}, 
{'base':'l', 'letters':/[lⓛｌŀĺľḷḹļḽḻſłƚɫⱡꝉꞁꝇ]/g}, 
{'base':'lj','letters':/[ǉ]/g}, 
{'base':'m', 'letters':/[mⓜｍḿṁṃɱɯ]/g}, 
{'base':'n', 'letters':/[nⓝｎǹńñṅňṇņṋṉƞɲŉꞑꞥ]/g}, 
{'base':'nj','letters':/[ǌ]/g}, 
{'base':'o', 'letters':/[oⓞｏòóôồốỗổõṍȭṏōṑṓŏȯȱöȫỏőǒȍȏơờớỡởợọộǫǭøǿɔꝋꝍɵ]/g}, 
{'base':'oi','letters':/[ƣ]/g}, 
{'base':'ou','letters':/[ȣ]/g}, 
{'base':'oo','letters':/[ꝏ]/g}, 
{'base':'p','letters':/[pⓟｐṕṗƥᵽꝑꝓꝕ]/g}, 
{'base':'q','letters':/[qⓠｑɋꝗꝙ]/g}, 
{'base':'r','letters':/[rⓡｒŕṙřȑȓṛṝŗṟɍɽꝛꞧꞃ]/g}, 
{'base':'s','letters':/[sⓢｓśṥŝṡšṧṣṩșşȿꞩꞅẛ]/g}, 
{'base':'ss','letters':/[ß]/g}, 
{'base':'t','letters':/[tⓣｔṫẗťṭțţṱṯŧƭʈⱦꞇ]/g}, 
{'base':'tz','letters':/[ꜩ]/g}, 
{'base':'u','letters':/[uⓤｕùúûũṹūṻŭüǜǘǖǚủůűǔȕȗưừứữửựụṳųṷṵʉ]/g}, 
{'base':'v','letters':/[vⓥｖṽṿʋꝟʌ]/g}, 
{'base':'vy','letters':/[ꝡ]/g}, 
{'base':'w','letters':/[wⓦｗẁẃŵẇẅẘẉⱳ]/g}, 
{'base':'x','letters':/[xⓧｘẋẍ]/g}, 
{'base':'y','letters':/[yⓨｙỳýŷỹȳẏÿỷẙỵƴɏỿ]/g}, 
{'base':'z','letters':/[zⓩｚźẑżžẓẕƶȥɀⱬꝣ]/g} ];
for(var i=0; i<defaultDiacriticsRemovalMap.length; i++) {
str = str.replace(defaultDiacriticsRemovalMap[i].letters, defaultDiacriticsRemovalMap[i].base);
}
return str;
}
""";