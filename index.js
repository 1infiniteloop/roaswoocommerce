const moment = require("moment-timezone");
const { pick, map, pipe, values, head, identity, of, keys, curry, not, sum, flatten } = require("ramda");
const { size, isUndefined, isEmpty, toNumber, orderBy: lodashorderby } = require("lodash");
const { from, zip, Observable, of: rxof, iif, catchError, throwError } = require("rxjs");
const { concatMap, map: rxmap, filter: rxfilter, tap, reduce: rxreduce, defaultIfEmpty } = require("rxjs/operators");
const { query, where, getDocs, collection } = require("firebase/firestore");
const { db } = require("./database");
const { logroupby, lokeyby, pipeLog, louniqby, lomap, isEmail, isIPv4, isIPv6, lohas, loreduce, lofilter, losortby, loorderby } = require("helpers");
const { get, all, mod, into, matching } = require("shades");
const { Facebook: RoasFacebook } = require("roasfacebook");

const Timestamp = {};

Timestamp.toUTCDigit = (timestamp) => {
    let regex_expression = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/;

    let date = moment(timestamp, "X").format("YYYY-MM-DD");
    let date_is_valid = regex_expression.test(date);

    if (!date_is_valid) {
        return timestamp / 1000;
    } else {
        return timestamp;
    }
};

const Facebook = {
    ads: {
        details: {
            get: ({ ad_ids, user_id, fb_ad_account_id, date } = {}) => {
                let func_name = `Facebook:ads:details`;
                console.log(func_name);

                if (!ad_ids) return throwError(`error:${func_name}:no ad_ids`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(ad_ids).pipe(
                    concatMap((ad_meta_data) => {
                        let { ad_id, timestamp } = ad_meta_data;
                        let ad_args = { ad_id, date, user_id, fb_ad_account_id };

                        return Facebook.ad.db.get(ad_args).pipe(
                            concatMap((ad) => iif(() => !isEmpty(ad), rxof(ad), Facebook.ad.api.get(ad_args))),
                            rxmap((ad) => ({ ...ad, timestamp })),
                            rxfilter(pipe(isEmpty, not))
                        );
                    })
                );
            },
        },
    },

    ad: {
        details: (ad) => {
            let func_name = `Facebook:ad:details`;
            console.log(func_name);

            if (ad.details) {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.details.ad_id,
                    asset_name: ad.details.ad_name,
                    campaign_id: ad.details.campaign_id,
                    campaign_name: ad.details.campaign_name,
                    adset_id: ad.details.adset_id,
                    adset_name: ad.details.adset_name,
                    ad_id: ad.details.ad_id,
                    ad_name: ad.details.ad_name,
                    name: ad.details.ad_name,
                };
            } else {
                return {
                    account_id: ad.account_id,
                    asset_id: ad.id,
                    asset_name: ad.name,
                    campaign_id: ad.campaign_id,
                    campaign_name: ad.campaign_name,
                    adset_id: ad.adset_id,
                    adset_name: ad.adset_name,
                    ad_id: ad.id,
                    ad_name: ad.name,
                    name: ad.name,
                };
            }
        },

        db: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:db:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                return from(RoasFacebook({ user_id }).ad.get_from_db({ ad_id })).pipe(
                    concatMap(identity),
                    rxfilter((ad) => !isEmpty(ad)),
                    rxmap(Facebook.ad.details),
                    defaultIfEmpty({}),
                    catchError((error) => rxof({ ad_id, error: true }))
                );
            },
        },

        api: {
            get: ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
                let func_name = `Facebook:ad:api:get`;
                console.log(func_name);

                if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
                if (!date) return throwError(`error:${func_name}:no date`);
                if (!user_id) return throwError(`error:${func_name}:no user_id`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let facebook = RoasFacebook({ user_id });

                return from(facebook.ad.get({ ad_id, date, fb_ad_account_id })).pipe(
                    rxmap(pipe(values, head)),
                    rxfilter((ad) => !isUndefined(ad.id)),
                    concatMap((ad) => {
                        let adset = Facebook.ad.adset.api.get({ adset_id: ad.adset_id, user_id, date, fb_ad_account_id });
                        let campaign = Facebook.ad.campaign.api.get({ campaign_id: ad.campaign_id, user_id, date, fb_ad_account_id });

                        return zip([adset, campaign]).pipe(
                            rxmap(([{ name: adset_name }, { name: campaign_name }]) => ({ ...ad, adset_name, campaign_name })),
                            rxmap(Facebook.ad.details)
                        );
                    }),
                    defaultIfEmpty({}),
                    catchError((error) => rxof({ ad_id, error: true }))
                );
            },
        },

        adset: {
            api: {
                get: ({ adset_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:adset:api:get`;
                    console.log(func_name);

                    if (!adset_id) return throwError(`error:${func_name}:no adset_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook = RoasFacebook({ user_id });

                    return from(facebook.adset.get({ adset_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)), defaultIfEmpty({}));
                },
            },
        },

        campaign: {
            api: {
                get: ({ campaign_id, user_id, date, fb_ad_account_id } = {}) => {
                    let func_name = `Facebook:ad:campaign:api:get`;
                    console.log(func_name);

                    if (!campaign_id) return throwError(`error:${func_name}:no campaign_id`);
                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!user_id) return throwError(`error:${func_name}:no user_id`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook = RoasFacebook({ user_id });

                    return from(facebook.campaign.get({ campaign_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)));
                },
            },
        },
    },
};

const Event = {
    ad: {
        id: ({ fb_ad_id, h_ad_id, ad_id } = {}) => {
            let func_name = `Event:ad:id`;
            console.log(func_name);

            if (ad_id) {
                return ad_id;
            }

            if (fb_ad_id && h_ad_id) {
                if (fb_ad_id == h_ad_id) {
                    return fb_ad_id;
                }

                if (fb_ad_id !== h_ad_id) {
                    return h_ad_id;
                }
            }

            if (fb_ad_id && !h_ad_id) {
                return fb_ad_id;
            }

            if (h_ad_id && !fb_ad_id) {
                return h_ad_id;
            }
        },
    },

    get_utc_timestamp: (value) => {
        console.log("get_utc_timestamp");

        let timestamp;

        if (get("created_at_unix_timestamp")(value)) {
            timestamp = get("created_at_unix_timestamp")(value);
            console.log(timestamp);
            return timestamp;
        }

        if (get("utc_unix_time")(value)) {
            let timestamp = get("utc_unix_time")(value);
            console.log(timestamp);
            return timestamp;
        }

        if (get("utc_iso_datetime")(value)) {
            let timestamp = pipe(get("utc_unix_time"), (value) => moment(value).unix())(value);
            console.log(timestamp);
            return timestamp;
        }

        timestamp = get("unix_datetime")(value);
        console.log(timestamp);

        if (!timestamp) {
            console.log("notimestamp");
            console.log(value);
        }

        return timestamp;
    },
};

const Events = {
    user: {
        get: {
            ipv4: ({ roas_user_id, ip }) => {
                let func_name = "Events:user:get:ipv4";
                console.log(func_name);
                let events_query = query(collection(db, "user_events"), where("roas_user_id", "==", roas_user_id), where("ipv4", "==", ip));
                return from(getDocs(events_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },

            ipv6: ({ roas_user_id, ip }) => {
                let func_name = "Events:user:get:ipv6";
                console.log(func_name);
                let events_query = query(collection(db, "user_events"), where("roas_user_id", "==", roas_user_id), where("ipv6", "==", ip));
                return from(getDocs(events_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));
            },
        },
    },
};

const Woocommerce = {
    utilities: {
        getDates: (startDate, endDate) => {
            const dates = [];
            let currentDate = startDate;
            const addDays = function (days) {
                const date = new Date(this.valueOf());
                date.setDate(date.getDate() + days);
                return date;
            };
            while (currentDate <= endDate) {
                dates.push(currentDate);
                currentDate = addDays.call(currentDate, 1);
            }
            return dates;
        },

        get_dates_range_array: (since, until) => {
            let start_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(since);

            let end_date = pipe(
                split("-"),
                map(toNumber),
                mod(1)((value) => value - 1),
                (value) => new Date(...value)
            )(until);

            const dates = pipe(
                ([start_date, end_date]) => Rules.utilities.getDates(start_date, end_date),
                mod(all)((date) => moment(date, "YYYY-MM-DD").format("YYYY-MM-DD"))
            )([start_date, end_date]);

            return dates;
        },

        date_pacific_time: (date, timezone = "America/Los_Angeles") => moment(date).tz(timezone),

        date_start_end_timestamps: (
            start = moment().format("YYYY-MM-DD"),
            end = moment().format("YYYY-MM-DD"),
            timezone = "America/Los_Angeles"
        ) => ({
            start: moment(Woocommerce.utilities.date_pacific_time(start, timezone)).add(1, "days").startOf("day").valueOf(),
            end: moment(Woocommerce.utilities.date_pacific_time(end, timezone)).add(1, "days").endOf("day").valueOf(),
        }),
    },

    orders: {
        get: ({ user_id, date }) => {
            let func_name = "Woocommerce:orders:get";
            console.log(func_name);

            if (!user_id) return throwError(`error:${func_name}:no user_id`);
            if (!date) return throwError(`error:${func_name}:no date`);

            let { start: start_timestamp, end: end_timestamp } = Woocommerce.utilities.date_start_end_timestamps(date, date);

            return from(
                getDocs(
                    query(
                        collection(db, "woocommerce"),
                        where("roas_user_id", "==", user_id),
                        where("created_at_unix_timestamp", ">", start_timestamp),
                        where("created_at_unix_timestamp", "<", end_timestamp)
                        // limit(100)
                    )
                )
            ).pipe(
                rxmap((data) => data.docs.map((doc) => doc.data())),
                rxmap(lofilter((order) => order.customer_ip_address)),
                rxmap(
                    pipe(
                        mod(all)(
                            pick([
                                "customer_id",
                                "customer_ip_address",
                                "total_tax",
                                "number",
                                "shipping_total",
                                "created_at_unix_timestamp",
                                "line_items",
                                "currency",
                                "total",
                                "status",
                                "meta_data",
                                "created_at_unix_timestamp",
                                "roas_user_id",
                                "billing",
                            ])
                        ),
                        mod(all, "meta_data")(lokeyby("key")),
                        mod(all, "meta_data")(pick(["billing_email", "confirm_billing_email", "_stripe_customer_id"])),
                        mod(all, "meta_data", all)(({ value }) => value),
                        mod(all)(({ meta_data, ...order }) => ({ ...order, ...meta_data })),
                        mod(all)(({ billing, ...order }) => ({
                            ...order,
                            first_name: billing.first_name,
                            last_name: billing.last_name,
                            email: billing.email,
                        }))
                    )
                ),
                defaultIfEmpty([])
            );
        },
    },

    order: {
        events: {
            get: (order) => {
                let func_name = `Woocommerce:order:events:get`;
                console.log(func_name);

                let ipv4_events = Events.user.get.ipv4({ ip: order.customer_ip_address, roas_user_id: order.roas_user_id });
                let ipv6_events = Events.user.get.ipv6({ ip: order.customer_ip_address, roas_user_id: order.roas_user_id });

                return zip([ipv4_events, ipv6_events]).pipe(
                    rxmap(([ipv4, ipv6]) => [...ipv4, ...ipv6]),
                    defaultIfEmpty([])
                );
            },
        },

        ads: {
            get: (order) => {
                let func_name = `Woocommerce:order:ads:get`;
                console.log(func_name);

                return Woocommerce.order.events.get(order).pipe(
                    concatMap(identity),
                    rxmap((event) => ({
                        ad_id: Event.ad.id(event),
                        timestamp: Math.trunc(Timestamp.toUTCDigit(Math.trunc(Event.get_utc_timestamp(event)))),
                    })),
                    rxmap(of),
                    rxreduce((prev, curr) => [...prev, ...curr]),
                    rxmap(lofilter((event) => !isUndefined(event.ad_id))),
                    rxfilter((ads) => !isEmpty(ads)),
                    rxmap(louniqby("ad_id")),
                    rxmap(loorderby(["timestamp"], ["desc"])),
                    defaultIfEmpty([])
                );
            },
        },

        cart: {
            items: (order) => {
                let func_name = `Woocommerce:order:cart:items`;
                console.log(func_name);

                return pipe(
                    get("line_items"),
                    mod(all)(({ price, name, total_tax }) => ({ price: Number(price) + Number(total_tax), name }))
                )(order);
            },
        },

        stats: {
            get: (order) => {
                let func_name = `Woocommerce:order:stats:get`;
                console.log(func_name);

                let cart = Woocommerce.order.cart.items(order);

                return { roassales: size(cart), roasrevenue: pipe(get(all, "price"), sum)(cart) };
            },
        },
    },

    customer: {
        normalize: (orders) => {
            let customer_id = pipe(get(all, "customer_id"), head)(orders);
            let customer_ip_address = pipe(get(all, "customer_ip_address"), head)(orders);
            let line_items = pipe(get(all, "line_items"), flatten)(orders);
            let roas_user_id = pipe(get(all, "roas_user_id"), head)(orders);
            let first_name = pipe(get(all, "first_name"), head)(orders);
            let last_name = pipe(get(all, "last_name"), head)(orders);
            let email = pipe(get(all, "email"), head)(orders);

            return {
                customer_id,
                customer_ip_address,
                line_items,
                roas_user_id,
                first_name,
                last_name,
                email,
            };
        },
    },

    report: {
        get: ({ user_id, date, fb_ad_account_id }) => {
            let func_name = `Woocommerce:report:get`;
            console.log(func_name);

            return Woocommerce.orders.get({ user_id, date }).pipe(
                rxmap(logroupby("customer_ip_address")),
                rxmap(mod(all)(Woocommerce.customer.normalize)),
                rxmap(values),
                concatMap(identity),
                rxmap((order) => ({
                    ...order,
                    cart: Woocommerce.order.cart.items(order),
                    stats: Woocommerce.order.stats.get(order),
                })),
                concatMap((order) =>
                    Woocommerce.order.ads.get(order).pipe(
                        rxfilter(pipe(isEmpty, not)),
                        rxmap((ads) => ({ ...order, ads }))
                    )
                ),
                concatMap((order) => {
                    let { ads: ad_ids, email } = order;

                    let ads = Facebook.ads.details.get({ ad_ids, fb_ad_account_id, user_id, date }).pipe(
                        rxfilter((ad) => !isUndefined(ad.asset_id)),
                        rxmap((ad) => ({ ...ad, email })),
                        rxmap(of),
                        rxreduce((prev, curr) => [...prev, ...curr]),
                        defaultIfEmpty([])
                    );

                    return from(ads).pipe(rxmap((ads) => ({ ...order, ads, email })));
                }),
                rxmap(pick(["email", "cart", "ads", "stats"])),
                rxmap(of),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(get(matching({ ads: (ads) => !isEmpty(ads) }))),
                rxmap(lokeyby("email")),
                rxmap((customers) => ({ customers })),
                rxmap((customers) => ({
                    ...customers,
                    date,
                    user_id,
                })),
                catchError((error) => rxof(error)),
                defaultIfEmpty({ date, customers: {}, user_id })
            );
        },
    },
};

exports.Woocommerce = Woocommerce;
