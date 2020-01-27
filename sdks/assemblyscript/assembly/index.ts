/** Copyright 2018-2020 Cargill Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
    get_state,
    set_state,
    delete_state,
    get_ptr_len,
    alloc,
    read_byte,
    write_byte,
    get_ptr_collection_len,
    add_to_collection,
    create_collection,
    invoke_smart_permission,
    log_buffer,
    log_level,
    get_ptr_from_collection
} from "./env";

/**
 * An entry in state containing the address and corresponding data.
 */
export class StateEntry {

    address: string;
    data: Uint8Array;

    constructor(address: string, data: Uint8Array) {
        this.address = address;
        this.data = data;
    }

    getAddress(): string {
        return this.address;
    }

    getData(): Uint8Array {
        return this.data;
    }
}

/**
 * A WasmBuffer is a wrapper around a wasm pointer.
 *
 * It contains a raw wasm pointer to location in executor
 * memory and a bytes representation of it's contents.
 *
 * It offers methods for accessing the data stored at the
 * location referenced by the raw pointer.
 */
export class WasmBuffer {

    raw: i32;
    data: Uint8Array;

    constructor(raw: i32, data: Uint8Array) {
        this.raw = raw;
        this.data = data;
    }

    static init(buffer: ArrayBuffer): WasmBuffer {
        const data = Uint8Array.wrap(buffer);
        const raw = alloc(data.length);

        for (let i = 0; i < data.length; i++) {
            if (write_byte(raw, i, data[i]) < 0) {
                const err = "Failed to write data to host memory";
                error(err);
                throw new Error(err);
            }
        }

        return new WasmBuffer(raw, data);
    }

    static fromRaw(raw: i32): WasmBuffer {
        const data = ptrToVec(raw);
        return new WasmBuffer(raw, data);
    }

    static fromList(ptr: i32): Array<WasmBuffer>  {
        const array = new Array<WasmBuffer>(0);

        if (ptr >= 0) {
            for (let i = 0; i < get_ptr_collection_len(ptr); i++) {
                const p = get_ptr_from_collection(ptr, i);
                if (p < 0) {
                    const err = "Pointer not found";
                    error(err);
                    throw new Error(err);
                }
                array.push(this.fromRaw(p));
            }
        }

        return array;
    }

    getRaw(): i32 {
        return this.raw;
    }

    getData(): Uint8Array {
        return this.data;
    }

    toString(): string {
        return String.UTF8.decode(this.data.buffer);
    }
}

function ptrToVec(ptr: i32): Uint8Array {
    const buffer = new Uint8Array(get_ptr_len(ptr));

    for (let i = 0; i < get_ptr_len(ptr); i++) {
        buffer[i] = read_byte(ptr + i);
    }

    return buffer;
}

export class Header {
    signer: string;
    constructor(signer: string) {
        this.signer = signer;
    }
    getSignerPublicKey(): string {
        return this.signer;
    }
}

export class TpProcessRequest {
    payload: Uint8Array;
    header: Header;
    signature: string;
    
    constructor(payload: Uint8Array, header: Header, signature: string) {
        this.payload = payload;
        this.header = header;
        this.signature = signature;
    }

    getPayload(): Uint8Array {
        return this.payload;
    }

    getHeader(): Header {
        return this.header;
    }

    getSignature(): string {
        return this.signature;
    }
}

export class TransactionContext {
    constructor() {}


    /** 
     * getStateEntries queries the validator state for data at each of the
     * addresses in the given list. The addresses that have been set
     * are returned.
     *
     * # Arguments
     *
     * * `addresses` - the addresses to fetch
     */
    getStateEntries(addresses: string[]): Array<StateEntry> {
        if (!addresses.length) {
            const err = "No address to get";
            error(err)
            throw new Error(err);
        }

        const head = addresses[0];
        const headerAddressBuffer = WasmBuffer.init(String.UTF8.encode(head));
        create_collection(headerAddressBuffer.getRaw());

        for (let i = 1; i < addresses.length; i++) {
            const wasmBuffer = WasmBuffer.init(String.UTF8.encode(addresses[i]));
            add_to_collection(headerAddressBuffer.getRaw(), wasmBuffer.getRaw());
        }

        const results = WasmBuffer.fromList(get_state(headerAddressBuffer.getRaw()));

        if ((results.length % 2) != 0) {
            const err = "Get state returned incorrect data fmt";
            error(err);
            throw new Error(err);
        }

        const resultsArray = new Array<StateEntry>(0);
        for (let i = 0; i < results.length; i += 2) {
            const addr = results[i].toString();
            resultsArray.push(new StateEntry(addr, results[i + 1].getData()));
        }

        return resultsArray;
    }

    /**
     * setStateEntries requests that each address in the provided map be
     * set in validator state to its corresponding value.
     *
     * # Arguments
     *
     * * `entries` - a list of state entries
     */
    setStateEntries(entries: Array<StateEntry>): void {
        const head = entries[0];

        const headAddressBuffer = WasmBuffer.init(String.UTF8.encode(head.getAddress()));
        create_collection(headAddressBuffer.getRaw());

        const wasmHeadDataBuffer = WasmBuffer.init(head.getData().buffer);
        add_to_collection(headAddressBuffer.getRaw(), wasmHeadDataBuffer.getRaw());


        for (let i = 1; i < entries.length; i++) {
            const address = entries[i].getAddress();
            const wasmAddrBuffer = WasmBuffer.init(String.UTF8.encode(address));
            add_to_collection(headAddressBuffer.getRaw(), wasmAddrBuffer.getRaw());

            const wasmDataBuffer = WasmBuffer.init(entries[i].getData().buffer);
            add_to_collection(headAddressBuffer.getRaw(), wasmDataBuffer.getRaw());
        }

        const result = set_state(headAddressBuffer.getRaw());

        info("State result: " + result.toString());

        if (result == 0) {
            const err = "Unable to set state";
            error(err);
            throw new Error(err);
        }
    }

    /** 
     * deleteStateEntries requests that each of the provided addresses be unset
     * in validator state. A list of successfully deleted addresses
     * is returned.
     *
     * # Arguments
     * * `addresses` - the addresses to delete
     */
    deleteStateEntries(addresses: string[]): Array<string> {
        if (!addresses.length) {
            const err = "No address to delete";
            error(err);
            throw new Error(err);
        }

        const head = addresses[0];
        const headerAddressBuffer = WasmBuffer.init(String.UTF8.encode(head));
        create_collection(headerAddressBuffer.getRaw());

        for (let i = 1; i < addresses.length; i++) {
            const wasmBuffer = WasmBuffer.init(String.UTF8.encode(addresses[i]));
            add_to_collection(headerAddressBuffer.getRaw(), wasmBuffer.getRaw());
        }

        const result = WasmBuffer.fromList(delete_state(headerAddressBuffer.getRaw()));
        const resultArray = new Array<string>(0);

        for (let i = 0; i < result.length; i++) {
            resultArray.push(result[i].toString());
        }

        return resultArray;
    }
}

export function invokeSmartPermission(
    contractAddr: string,
    name: string,
    roles: Array<string>,
    orgId: string,
    publicKey: string,
    payload: ArrayBuffer
): i32 {
    if (!roles.length) {
        const err = "No roles set";
        error(err);
        throw new Error(err);
    }

    const head = roles[0];

    const headerRoleBuffer = WasmBuffer.init(String.UTF8.encode(head));
    create_collection(headerRoleBuffer.getRaw());

    for (let i = 1; i < roles.length; i++) {
        const wasmBuffer = WasmBuffer.init(String.UTF8.encode(roles[i]));
        add_to_collection(headerRoleBuffer.getRaw(), wasmBuffer.getRaw());
    }

    const contractAddrBuffer = WasmBuffer.init(String.UTF8.encode(contractAddr));
    const nameBuffer = WasmBuffer.init(String.UTF8.encode(name));
    const orgIdBuffer = WasmBuffer.init(String.UTF8.encode(orgId));
    const publicKeyBuffer = WasmBuffer.init(String.UTF8.encode(publicKey));
    const payloadBuffer = WasmBuffer.init(payload);

    return invoke_smart_permission(
        contractAddrBuffer.getRaw(),
        nameBuffer.getRaw(),
        headerRoleBuffer.getRaw(),
        orgIdBuffer.getRaw(),
        publicKeyBuffer.getRaw(),
        payloadBuffer.getRaw()
    );
}

/**
 * executes entry point, returns 1 if successful and 0 otherwise.
 */
export function executeEntryPoint(
    payloadPtr: i32,
    signerPtr: i32,
    signaturePtr: i32,
    apply: (x:TpProcessRequest, y: TransactionContext) => bool
): i32 {
    const payload = WasmBuffer.fromRaw(payloadPtr);
    const signer = WasmBuffer.fromRaw(signerPtr);
    const signature = WasmBuffer.fromRaw(signaturePtr);

    const header = new Header(signer.toString());

    const context = new TransactionContext();
    const request = new TpProcessRequest(payload.getData(), header, signature.toString());

    return apply(request, context) ? 1 : 0;
}

export class Request {
    roles: Array<string>;
    orgId: string;
    publicKey: string;
    payload: Uint8Array;

    constructor(roles: Array<string>, orgId: string, publicKey: string, payload: Uint8Array) {
        this.roles = roles;
        this.orgId = orgId;
        this.publicKey = publicKey;
        this.payload = payload;
    }

    getRoles(): Array<string> {
        return this.roles;
    }

    getOrgId(): string {
        return this.orgId; 
    }

    getPublicKey(): string {
        return this.publicKey;
    }

    getState(address: string): Uint8Array {
        const wasmBuffer = WasmBuffer.init(String.UTF8.encode(address));
        return ptrToVec(get_state(wasmBuffer.getRaw()));
    }

    getPayload(): Uint8Array {
        return this.payload;
    }
}

/**
 * executes entry point, returns 1 if successful and 0 otherwise.
 */
export function executeSmartPermissionEntryPoint(
    rolesPtr: i32,
    orgIdPtr: i32,
    publicKeyPtr: i32,
    payloadPtr: i32,
    hasPermission: (x: Request) => bool
): i32 {
    const roles = WasmBuffer.fromList(rolesPtr).map<string>((x) => x.toString());
    const orgId = WasmBuffer.fromRaw(orgIdPtr).toString();
    const publicKey = WasmBuffer.fromRaw(publicKeyPtr).toString();
    const payload = WasmBuffer.fromRaw(payloadPtr).getData(); 

    const request = new Request(roles, orgId, publicKey, payload);

    return hasPermission(request) ? 1 : 0;
}

export function trace(msg: string): void {
    log(LogLevel.TraceLv, msg);
}

export function debug(msg: string): void {
    log(LogLevel.DebugLv, msg);
}

export function info(msg: string): void {
    log(LogLevel.InfoLv, msg);
} 

export function warn(msg: string): void {
    log(LogLevel.WarnLv, msg);
}

export function error(msg: string): void {
    log(LogLevel.ErrorLv, msg);
}

export function logLevel(): LogLevel {
    switch (log_level()) {
        case 4:
            return LogLevel.TraceLv;
        case 3:
            return LogLevel.DebugLv;
        case 2:
            return LogLevel.InfoLv;
        case 1:
            return LogLevel.WarnLv;
        case 0:
            return LogLevel.ErrorLv;
        default:
            throw new Error("Invalid log level returned from environment");
    };
}

export function logEnabled(lvl: LogLevel): bool {
    return lvl >= logLevel();
}

/**
 * Source: Chainsafe as-sha-256
 */
export function toHexString(bin: ArrayBuffer): string {
    const u8Bin = Uint8Array.wrap(bin);
    const bin_len = u8Bin.length;
    let hex = "";

    for (let i = 0; i < bin_len; i++) {
        let bin_i = u8Bin[i] as u32;

        // Bit shifting needed because `toString` in
        // assembly script does not support radix argument
        let c = bin_i & 0xf;
        let b = bin_i >> 4;

        // Calculate ASCII code
        let x: u32 = ((87 + c + (((c - 10) >> 8) & ~38)) << 8) |
            (87 + b + (((b - 10) >> 8) & ~38));

        hex += String.fromCharCode(x as u8);
        x >>= 8;
        hex += String.fromCharCode(x as u8);
    }
    return hex;
}

enum LogLevel {
    TraceLv = 4,
    DebugLv = 3,
    InfoLv = 2,
    WarnLv = 1, 
    ErrorLv = 0,
}


function log(level: LogLevel, msg: string): void {
    const wasmBuffer = WasmBuffer.init(String.UTF8.encode(msg));
    const ptr = wasmBuffer.getRaw();
    switch (level) {
        case LogLevel.TraceLv:
            log_buffer(4, ptr);
            break;
        case LogLevel.DebugLv:
            log_buffer(3, ptr);
            break;
        case LogLevel.InfoLv:
            log_buffer(2, ptr);
            break;
        case LogLevel.WarnLv:
            log_buffer(1, ptr);
            break;
        case LogLevel.ErrorLv:
            log_buffer(0, ptr);
            break;
    } 
}
