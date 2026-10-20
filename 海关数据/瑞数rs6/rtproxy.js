
rt_loginfo = false;
(() => {
    const origin_log = console.log;
    rt_log = function () {
        if (rt_loginfo) {
            return origin_log(...arguments);
        }
    }
})();
const defaultFilterProps=['Math','Proxy','Promise','Array','isNaN','encodeURI','Uint8Array','toJSON','parseInt',Symbol.toPrimitive,'String','Object','Symbol','ActiveXObject','mozHidden','attachEvent','Date','_phantom','domAutomation','XDomainRequest','emit','spawn','callPhantom','Buffer','__nightmare','_Selenium_IDE_Recorder','__$webdriverAsyncExecutor','__driver_evaluate','__driver_unwrapped','__fxdriver_evaluate','__fxdriver_unwrapped','__lastWatirAlert','__lastWatirConfirm','__lastWatirPrompt','__phantomas','__selenium_evaluate','__selenium_unwrapped','__webdriverFuncgeb','__webdriver__chr','__webdriver_evaluate','__webdriver_script_fn','__webdriver_script_func','__webdriver_unwrapped','callSelenium','calledPhantom','calledSelenium','domAutomationController','watinExpressionError','watinExpressionResult','spynner_additional_js_loaded','$chrome_asyncScriptInfo','__webdriver_script_function','SharedArrayBuffer','documentMode','opera','InstallTrigger','mozInnerScreenY','RegExp'];


var existsobj = {
    'windowEnumerable': ["window","self","document","name","location","customElements","history","navigation","locationbar","menubar","personalbar","scrollbars","statusbar","toolbar","status","closed","frames","length","top","opener","parent","frameElement","navigator","origin","external","screen","innerWidth","innerHeight","scrollX","pageXOffset","scrollY","pageYOffset","visualViewport","screenX","screenY","outerWidth","outerHeight","devicePixelRatio","event","clientInformation","screenLeft","screenTop","styleMedia","onsearch","trustedTypes","performance","onappinstalled","onbeforeinstallprompt","crypto","indexedDB","sessionStorage","localStorage","onbeforexrselect","onabort","onbeforeinput","onbeforematch","onbeforetoggle","onblur","oncancel","oncanplay","oncanplaythrough","onchange","onclick","onclose","oncontentvisibilityautostatechange","oncontextlost","oncontextmenu","oncontextrestored","oncuechange","ondblclick","ondrag","ondragend","ondragenter","ondragleave","ondragover","ondragstart","ondrop","ondurationchange","onemptied","onended","onerror","onfocus","onformdata","oninput","oninvalid","onkeydown","onkeypress","onkeyup","onload","onloadeddata","onloadedmetadata","onloadstart","onmousedown","onmouseenter","onmouseleave","onmousemove","onmouseout","onmouseover","onmouseup","onmousewheel","onpause","onplay","onplaying","onprogress","onratechange","onreset","onresize","onscroll","onsecuritypolicyviolation","onseeked","onseeking","onselect","onslotchange","onstalled","onsubmit","onsuspend","ontimeupdate","ontoggle","onvolumechange","onwaiting","onwebkitanimationend","onwebkitanimationiteration","onwebkitanimationstart","onwebkittransitionend","onwheel","onauxclick","ongotpointercapture","onlostpointercapture","onpointerdown","onpointermove","onpointerrawupdate","onpointerup","onpointercancel","onpointerover","onpointerout","onpointerenter","onpointerleave","onselectstart","onselectionchange","onanimationend","onanimationiteration","onanimationstart","ontransitionrun","ontransitionstart","ontransitionend","ontransitioncancel","onafterprint","onbeforeprint","onbeforeunload","onhashchange","onlanguagechange","onmessage","onmessageerror","onoffline","ononline","onpagehide","onpageshow","onpopstate","onrejectionhandled","onstorage","onunhandledrejection","onunload","isSecureContext","crossOriginIsolated","scheduler","alert","atob","blur","btoa","cancelAnimationFrame","cancelIdleCallback","captureEvents","clearInterval","clearTimeout","close","confirm","createImageBitmap","fetch","find","focus","getComputedStyle","getSelection","matchMedia","moveBy","moveTo","open","postMessage","print","prompt","queueMicrotask","releaseEvents","reportError","requestAnimationFrame","requestIdleCallback","resizeBy","resizeTo","scroll","scrollBy","scrollTo","setInterval","setTimeout","stop","structuredClone","webkitCancelAnimationFrame","webkitRequestAnimationFrame","chrome","caches","cookieStore","ondevicemotion","ondeviceorientation","ondeviceorientationabsolute","launchQueue","sharedStorage","documentPictureInPicture","fetchLater","getDigitalGoodsService","getScreenDetails","queryLocalFonts","showDirectoryPicker","showOpenFilePicker","showSaveFilePicker","originAgentCluster","onpageswap","onpagereveal","credentialless","fence","speechSynthesis","oncommand","onscrollend","onscrollsnapchange","onscrollsnapchanging","webkitRequestFileSystem","webkitResolveLocalFileSystemURL","arr","x","TEMPORARY","PERSISTENT","addEventListener","dispatchEvent","removeEventListener","when"],
    'documentEnumerable':  ["location","implementation","URL","documentURI","compatMode","characterSet","charset","inputEncoding","contentType","doctype","documentElement","xmlEncoding","xmlVersion","xmlStandalone","domain","referrer","cookie","lastModified","readyState","title","dir","body","head","images","embeds","plugins","links","forms","scripts","currentScript","defaultView","designMode","onreadystatechange","anchors","applets","fgColor","linkColor","vlinkColor","alinkColor","bgColor","all","scrollingElement","onpointerlockchange","onpointerlockerror","hidden","visibilityState","wasDiscarded","prerendering","featurePolicy","webkitVisibilityState","webkitHidden","onbeforecopy","onbeforecut","onbeforepaste","onfreeze","onprerenderingchange","onresume","onsearch","onvisibilitychange","timeline","fullscreenEnabled","fullscreen","onfullscreenchange","onfullscreenerror","webkitIsFullScreen","webkitCurrentFullScreenElement","webkitFullscreenEnabled","webkitFullscreenElement","onwebkitfullscreenchange","onwebkitfullscreenerror","rootElement","pictureInPictureEnabled","onbeforexrselect","onabort","onbeforeinput","onbeforematch","onbeforetoggle","onblur","oncancel","oncanplay","oncanplaythrough","onchange","onclick","onclose","oncontentvisibilityautostatechange","oncontextlost","oncontextmenu","oncontextrestored","oncuechange","ondblclick","ondrag","ondragend","ondragenter","ondragleave","ondragover","ondragstart","ondrop","ondurationchange","onemptied","onended","onerror","onfocus","onformdata","oninput","oninvalid","onkeydown","onkeypress","onkeyup","onload","onloadeddata","onloadedmetadata","onloadstart","onmousedown","onmouseenter","onmouseleave","onmousemove","onmouseout","onmouseover","onmouseup","onmousewheel","onpause","onplay","onplaying","onprogress","onratechange","onreset","onresize","onscroll","onsecuritypolicyviolation","onseeked","onseeking","onselect","onslotchange","onstalled","onsubmit","onsuspend","ontimeupdate","ontoggle","onvolumechange","onwaiting","onwebkitanimationend","onwebkitanimationiteration","onwebkitanimationstart","onwebkittransitionend","onwheel","onauxclick","ongotpointercapture","onlostpointercapture","onpointerdown","onpointermove","onpointerrawupdate","onpointerup","onpointercancel","onpointerover","onpointerout","onpointerenter","onpointerleave","onselectstart","onselectionchange","onanimationend","onanimationiteration","onanimationstart","ontransitionrun","ontransitionstart","ontransitionend","ontransitioncancel","oncopy","oncut","onpaste","children","firstElementChild","lastElementChild","childElementCount","activeElement","styleSheets","pointerLockElement","fullscreenElement","adoptedStyleSheets","pictureInPictureElement","fonts","adoptNode","append","captureEvents","caretRangeFromPoint","clear","close","createAttribute","createAttributeNS","createCDATASection","createComment","createDocumentFragment","createElement","createElementNS","createEvent","createExpression","createNSResolver","createNodeIterator","createProcessingInstruction","createRange","createTextNode","createTreeWalker","elementFromPoint","elementsFromPoint","evaluate","execCommand","exitFullscreen","exitPictureInPicture","exitPointerLock","getAnimations","getElementById","getElementsByClassName","getElementsByName","getElementsByTagName","getElementsByTagNameNS","getSelection","hasFocus","hasStorageAccess","hasUnpartitionedCookieAccess","importNode","moveBefore","open","prepend","queryCommandEnabled","queryCommandIndeterm","queryCommandState","queryCommandSupported","queryCommandValue","querySelector","querySelectorAll","releaseEvents","replaceChildren","requestStorageAccess","requestStorageAccessFor","startViewTransition","webkitCancelFullScreen","webkitExitFullscreen","write","writeln","fragmentDirective","browsingTopics","hasPrivateToken","hasRedemptionRecord","oncommand","onscrollend","onscrollsnapchange","onscrollsnapchanging","caretPositionFromPoint","nodeType","nodeName","baseURI","isConnected","ownerDocument","parentNode","parentElement","childNodes","firstChild","lastChild","previousSibling","nextSibling","nodeValue","textContent","ELEMENT_NODE","ATTRIBUTE_NODE","TEXT_NODE","CDATA_SECTION_NODE","ENTITY_REFERENCE_NODE","ENTITY_NODE","PROCESSING_INSTRUCTION_NODE","COMMENT_NODE","DOCUMENT_NODE","DOCUMENT_TYPE_NODE","DOCUMENT_FRAGMENT_NODE","NOTATION_NODE","DOCUMENT_POSITION_DISCONNECTED","DOCUMENT_POSITION_PRECEDING","DOCUMENT_POSITION_FOLLOWING","DOCUMENT_POSITION_CONTAINS","DOCUMENT_POSITION_CONTAINED_BY","DOCUMENT_POSITION_IMPLEMENTATION_SPECIFIC","appendChild","cloneNode","compareDocumentPosition","contains","getRootNode","hasChildNodes","insertBefore","isDefaultNamespace","isEqualNode","isSameNode","lookupNamespaceURI","lookupPrefix","normalize","removeChild","replaceChild","addEventListener","dispatchEvent","removeEventListener","when"],
    'navigatorEnumerable': ["vendorSub","productSub","vendor","maxTouchPoints","scheduling","userActivation","doNotTrack","geolocation","connection","plugins","mimeTypes","pdfViewerEnabled","webkitTemporaryStorage","webkitPersistentStorage","windowControlsOverlay","hardwareConcurrency","cookieEnabled","appCodeName","appName","appVersion","platform","product","userAgent","language","languages","onLine","webdriver","getGamepads","javaEnabled","sendBeacon","vibrate","deprecatedRunAdAuctionEnforcesKAnonymity","protectedAudience","bluetooth","storageBuckets","clipboard","credentials","keyboard","managed","mediaDevices","storage","serviceWorker","virtualKeyboard","wakeLock","deviceMemory","userAgentData","login","ink","mediaCapabilities","devicePosture","hid","locks","gpu","mediaSession","permissions","presentation","serial","usb","xr","adAuctionComponents","runAdAuction","canLoadAdAuctionFencedFrame","canShare","share","clearAppBadge","getBattery","getUserMedia","requestMIDIAccess","requestMediaKeySystemAccess","setAppBadge","webkitGetUserMedia","clearOriginJoinedAdInterestGroups","createAuctionNonce","joinAdInterestGroup","leaveAdInterestGroup","updateAdInterestGroups","deprecatedReplaceInURN","deprecatedURNToURL","getInstalledRelatedApps","getInterestGroupAdAuctionData","registerProtocolHandler","unregisterProtocolHandler"],
}

!(function () {
    rtwatch = function (obj, name) {
        return new Proxy(obj, {
            get(target, property, receiver) {
                if (name === 'window' && typeof property === 'string' && ['process','Buffer','require','module','exports','__dirname','__filename','setImmediate','clearImmediate','GLOBAL','root','global'].includes(property)) {
                    rt_log(`取值:`, name, '.', property, `=>`, undefined);
                    return undefined;
                }
                if (name)
                    if (defaultFilterProps.includes(property)) {
                        var val = Reflect.get(...arguments);
                        return val
                    }
                    else {
                        var val = Reflect.get(...arguments);
                        if (property === Symbol.for('nodejs.util.inspect.custom') || property === Symbol.for('debug.description')){
                            var val = Reflect.get(...arguments);
                            return val
                        }
                        if (name.indexOf('方法原型') !== -1 && name.indexOf('方法原型') !== undefined){
                            if (property === "0" || property === Symbol.for('nodejs.util.inspect.custom')) {
                                var val = Reflect.get(...arguments);
                                return val
                            }
                        }
                        if (typeof val === 'function') {
                            rt_log(`取值:`, name, '.', property, `=>`, 'function');
                        }
                        else {
                            rt_log(`取值:`, name, '.', property, `=>`, val);
                        }
                        if (name === 'window' && val === undefined){
                            if (existsobj['windowEnumerable'].includes(property)){
                                rt_log(`存在于=>`,'window', `没有补=>`, property, `undefined`);
                            }
                        }
                        if (name === 'document' && val === undefined){
                            if (existsobj['documentEnumerable'].includes(property)){
                                rt_log(`存在于=>`,'document', `没有补=>`, property, `undefined`);
                            }
                        }
                        if (name === 'navigator' && val === undefined){
                            if (existsobj['navigatorEnumerable'].includes(property)){
                                rt_log(`存在于=>`,'navigator', `没有补=>`, property, `undefined`);
                            }
                        }
                        return val
                    }
            },
            set(target, property, newValue, receiver) {
                var val = Reflect.set(...arguments)
                if (typeof newValue === 'function') {
                    rt_log(`设置值:${name}.${property}=>function`);
                }
                else {
                    rt_log(`设置值:${name}.${property}=>`,newValue);
                }
                return val
            },
            has(target, key) {
                if (key.toString() === 'Symbol(Symbol.iterator)'){
                    return key in target;
                }
                if (name === 'window' && ['process','Buffer','require','module','exports','__dirname','__filename','setImmediate','clearImmediate','GLOBAL','root','global'].includes(String(key))) {
                    rt_log(`检测属性存在性: ${name}.${key.toString()}`);
                    return false;
                }
                rt_log(`检测属性存在性: ${name}.${key.toString()}`);
                return key in target;
            },
            ownKeys(target) {
                rt_log(`ownKeys检测: ${name}`);
                const keys = Reflect.ownKeys(...arguments);
                if (name !== 'window') return keys;
                const hide = new Set(['process','Buffer','require','module','exports','__dirname','__filename','setImmediate','clearImmediate','GLOBAL','root','global']);
                return keys.filter((key) => !hide.has(String(key)));
            },
        })
    }
})();

const syString = Symbol('ToString');
(()=> {
	'use strict';
	const $toString = Function.toString
	function ToString() {
		return typeof this == 'function' && this[syString] || $toString.call(this)
	}
	function to_native(fun, key, value) {
		Object.defineProperty(fun, key, {
			value: value,
			writable: false, // 可选，设置为不可写
			enumerable: false, // 可选，设置为不可枚举
			configurable: true, // 可选，设置为可配置
		});
	}
	delete Function.prototype['toString'];
	to_native(Function.prototype, 'toString', ToString)
	to_native(Function.prototype.toString, syString, "function toString() { [native code] }");
	to_native(Function.prototype.toString, 'name', "toString");
	global.fun_to_native=function (fun) {
		to_native(fun, syString, `function ${fun.name}() { [native code] }`)
	}
})(global);

window = rtwatch(global,'window')

document = rtwatch({},'document')

location = rtwatch({},'location')

navigator = rtwatch({},'navigator')

screen= rtwatch({},'screen')

localStorage = rtwatch({},'localStorage')

sessionStorage = rtwatch({},'sessionStorage')


// 目标js
