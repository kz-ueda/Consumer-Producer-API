/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/**
 * Copyright (c) 2014-2016 Regents of the University of California.
 *
 * This file is part of Consumer/Producer API library.
 *
 * Consumer/Producer API library library is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public License as published by the Free 
 * Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * Consumer/Producer API library is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 *
 * You should have received copies of the GNU General Public License and GNU Lesser
 * General Public License along with Consumer/Producer API, e.g., in COPYING.md file.  If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * See AUTHORS.md for complete list of Consumer/Producer API authors and contributors.
 */


#ifndef CONSUMER_PRODUCER_TLV_HPP
#define CONSUMER_PRODUCER_TLV_HPP

#include <ndn-cxx/encoding/tlv.hpp>

namespace ndn {
namespace tlv {

using namespace ndn::tlv;

enum {
  ContentType_Manifest = 4,
  ManifestCatalogue  = 128,
  KeyValuePair  = 129
};

} // tlv
} // ndn

#endif // CONSUMER_PRODUCER_TLV_HPP
