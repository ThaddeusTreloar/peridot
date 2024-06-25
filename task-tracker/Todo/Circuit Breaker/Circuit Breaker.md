# Dependencies
[[Input Layers]]

# Description

An input layer that intercepts connectivity errors and applies some circuit break logic to the failed packets, returning Poll::Pending when circuit is Open(no traffic).