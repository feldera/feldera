export declare type ScrollBehavior = 'auto' | 'smooth'

export declare type GetKey = (index: number) => unknown

export declare type ScrollEvent = UIEvent & { currentTarget: EventTarget & HTMLDivElement }

export declare type OnScroll = (event: ScrollEvent) => void
