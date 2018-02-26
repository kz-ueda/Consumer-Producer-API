/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2014-2017 Regents of the University of California.
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

#ifndef BBR_DATA_RETRIEVAL_HPP
#define BBR_DATA_RETRIEVAL_HPP

#include "data-retrieval-protocol.hpp"
#include "rtt-estimator.hpp"
#include "selector-helper.hpp"

namespace ndn {

class BBROptions{
public:
  explicit BBROptions(){};
  ~BBROptions(){};
  double initCwnd = 1.0; ///< initial congestion window size
  double initSsthresh = std::numeric_limits<double>::max(); ///< initial slow start threshold
  double aiStep = 1.0; ///< additive increase step (in segments)
  double mdCoef = 0.5; ///< multiplicative decrease coefficient
  time::milliseconds rtoCheckInterval{10}; ///< interval for checking retransmission timer
  bool disableCwa = false; ///< disable Conservative Window Adaptation
  bool resetCwndToInit = false; ///< reduce cwnd to initCwnd when loss event occurs
};

class BBRDataRetrieval : public DataRetrievalProtocol
{
public:
  BBRDataRetrieval(Context* context,
                    const BBROptions& options = BBROptions());

  ~BBRDataRetrieval();

  void
  start();

  void
  stop();

  void
  getNetworkStatistics(double minRTT, double maxRTT, double currentWindow);

private:
  void
  sendInterest();

  void
  onData(const Interest& interest, const Data& data);

  void
  onNack(const Interest& interest, const lp::Nack& nack);

  void
  onTimeout(const Interest& interest);

  void
  onManifestData(const Interest& interest, const Data& data);

  void
  onNackData(const Interest& interest, const Data& data);

  void
  onContentData(const Interest& interest, const Data& data);

  void
  reassemble();

  void
  copyContent(const Data& data);

  bool
  referencesManifest(const Data& data);

  void
  retransmitFreshInterest(const Interest& interest);

  bool
  retransmitInterestWithExclude(const Interest& interest, const Data& dataSegment);

  bool
  retransmitInterestWithDigest(const Interest& interest, const Data& dataSegment, const Manifest& manifestSegment);

  bool
  verifySegmentWithManifest(const Manifest& manifestSegment, const Data& dataSegment);

  name::Component
  getDigestFromManifest(const Manifest& manifestSegment, const Data& dataSegment);

  void
  checkFastRetransmissionConditions(const Interest& interest);

  void
  fastRetransmit(const Interest& interest, uint64_t segNumber);

  void
  removeAllPendingInterests();

  void
  removeAllScheduledInterests();

  void
  paceInterests(int nInterests, time::milliseconds timeWindow);

  void
  increaseWindow();

  void
  decreaseWindow();

  void
  controlOutgoingInterests();

  time::steady_clock::TimePoint
  getStartTime() const
  {
      return m_startTime;
  }

private:
  Scheduler* m_scheduler;
  KeyChain m_keyChain;

  // reassembly variables
  bool m_isFinalBlockNumberDiscovered;
  uint64_t m_finalBlockNumber;
  uint64_t m_lastReassembledSegment;
  std::vector<uint8_t> m_contentBuffer;
  size_t m_contentBufferSize;

  // transmission variables
  double m_currentWindowSize;
  int m_interestsInFlight;
  uint64_t m_segNumber;
  std::unordered_map<uint64_t, int> m_interestRetransmissions;                       // by segment number
  std::unordered_map<uint64_t, const PendingInterestId*> m_expressedInterests;       // by segment number
  std::unordered_map<uint64_t, EventId> m_scheduledInterests;                        // by segment number
  std::unordered_map<uint64_t, time::steady_clock::time_point> m_interestTimepoints; // by segment
  RttEstimator m_rttEstimator;

  // buffers
  std::map<uint64_t, shared_ptr<const Data>> m_receiveBuffer;         // verified segments by segment number
  std::map<uint64_t, shared_ptr<const Data>> m_unverifiedSegments;    // used with embedded manifests
  std::map<uint64_t, shared_ptr<const Manifest>> m_verifiedManifests; // by segment number

  // Fast Retransmission
  std::map<uint64_t, bool> m_receivedSegments;
  std::unordered_map<uint64_t, bool> m_fastRetxSegments;

  const BBROptions m_options;
  double m_minRTT;
  double m_maxRTT;
  double m_ssthresh;    // slow start threshold
  time::steady_clock::TimePoint m_startTime;
  bool m_nowPacing;
  bool m_doLogging;
};

} // namespace ndn

#endif // BBR_DATA_RETRIEVAL_HPP
