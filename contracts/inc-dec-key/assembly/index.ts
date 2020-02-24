// Copyright 2018-2020 Cargill Incorporated
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


import { hash } from "@chainsafe/as-sha256";
import {
    TpProcessRequest,
    TransactionContext,
    StateEntry,
    executeEntryPoint,
    toHexString,
    info,
    error,
    Result,
    Void,
} from "sabre-sdk";

const NAMESPACE = "12cd3c"; 

function apply(req: TpProcessRequest, ctx: TransactionContext): bool {
    info(req.getPayload().toString());
    const _payload = String.UTF8.decode(req.getPayload().buffer);
    info("Payload: " + _payload);
    const payload = _payload.split(',');
    const action = payload[0];
    const name = payload[1];

    if (action == "init") {
        init(name, ctx);
    } else if (action == "inc") {
        const result = inc(name, ctx);
        if (result.isErr()) {
            error(result.getErr().getMessage());
        }
    } else if (action == "dec") {
        const result = dec(name, ctx);
        if (result.isErr()) {
            error(result.getErr().getMessage());
        }
    } else if (action == "remove") {
        remove(name, ctx);
    } else {
        return false;
    }

    return true;
}

export function entrypoint(payload: i32, signer: i32, signature: i32): i32  {
    return executeEntryPoint(payload, signer, signature, apply);
}

function computeAddress(name: string): string {
    let buffer = Uint8Array.wrap(String.UTF8.encode(name));
    const hash = hash(buffer);
    return NAMESPACE + toHexString(hash.buffer);
}

function init(name: string, ctx: TransactionContext): void {
    info("Initializing counter");
    const address = computeAddress(name);
    info("Creating address " + address);
    const init_value = Uint8Array.wrap(String.UTF8.encode((0).toString()));

    const array = new Array<StateEntry>(0);
    array.push(new StateEntry(address, init_value));

    ctx.setStateEntries(array);

    info("Counter: " + name + " initialized!");
}

function inc(name: string, ctx: TransactionContext): Result<Void> {
    const getStateArray = new Array<string>();
    const address = computeAddress(name);
    getStateArray.push(address);

    const entries = ctx.getStateEntries(getStateArray);
    
    if (!entries.length) {
        return Result.err<Void>("No state entries found for " + name);
    }

    let value = extractValue(entries[0]);
    info("old value of " + name + "  " + value.toString());
    value++;
    const value_bytes = Uint8Array.wrap(String.UTF8.encode(value.toString()));

    const setStateArray = new Array<StateEntry>(0);
    setStateArray.push(new StateEntry(address, value_bytes));

    ctx.setStateEntries(setStateArray);

    info(name + " new value: " + value.toString());

    return Result.okVoid();
}

function dec(name: string, ctx: TransactionContext): Result<Void> {
    const getStateArray = new Array<string>();
    const address = computeAddress(name);
    getStateArray.push(address);

    const entries = ctx.getStateEntries(getStateArray);
    
    if (!entries.length) {
        return Result.err<Void>("No state entries found for " + name);
    }

    let value = extractValue(entries[0]);
    info("old value of " + name + "  " + value.toString());
    value--;
    const value_bytes = Uint8Array.wrap(String.UTF8.encode(value.toString()));

    const setStateArray = new Array<StateEntry>(0);
    setStateArray.push(new StateEntry(address, value_bytes));

    ctx.setStateEntries(setStateArray);

    info(name + " new value: " + value.toString());

    return Result.okVoid();
}

function remove(name: string, ctx: TransactionContext): void {
    const deleteStateArray = new Array<string>();
    const address = computeAddress(name);
    deleteStateArray.push(address);

    ctx.deleteStateEntries(deleteStateArray);

    info(name + " has been deleted");
}

function extractValue(entry: StateEntry): i32 {
    const str = String.UTF8.decode(entry.getData().buffer);
    return Number.parseInt(str) as i32;
}
