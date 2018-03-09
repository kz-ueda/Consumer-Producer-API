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

#include "bbr-data-retrieval.hpp"
#include "consumer-context.hpp"

#include <ndn-cxx/security/signing-helpers.hpp>
#include <ndn-cxx/security/verification-helpers.hpp>

namespace ndn {

BBRDataRetrieval::BBRDataRetrieval(Context* context, const BBROptions& options)
  : DataRetrievalProtocol(context)
  , m_isFinalBlockNumberDiscovered(false)
  , m_finalBlockNumber(std::numeric_limits<uint64_t>::max())
  , m_lastReassembledSegment(0)
  , m_contentBufferSize(0)
  , m_currentWindowSize(0)
  , m_interestsInFlight(0)
  , m_segNumber(0)
  // ICN2020
  , m_options(options)
  , m_nowPacing(false)
  , m_doLogging(false)
  , m_minRTT(0)
  , m_maxRTT(0)
  , m_ssthresh(m_options.initSsthresh)
  , m_startTime(time::steady_clock::now())
{
  context->getContextOption(FACE, m_face);
  m_scheduler = new Scheduler(m_face->getIoService());
  m_context->getContextOption(LOGGING, m_doLogging);
}

BBRDataRetrieval::~BBRDataRetrieval()
{
  stop();
  delete m_scheduler;
}

void
BBRDataRetrieval::start()
{
  m_isRunning = true;
  m_isFinalBlockNumberDiscovered = false;
  m_finalBlockNumber = std::numeric_limits<uint64_t>::max();
  m_segNumber = 0;
  m_interestsInFlight = 0;
  m_lastReassembledSegment = 0;
  m_contentBufferSize = 0;
  m_contentBuffer.clear();
  m_interestRetransmissions.clear();
  m_receiveBuffer.clear();
  m_unverifiedSegments.clear();
  m_verifiedManifests.clear();

  // Manifest-based retrieval
  // Import finalBlockNumber from consumer context
  // @ToDo: Manifest取得&contextへの登録は別出しした方が整理しやすいかもしれない
  int finalBlockNumberFromContext = -1;
  m_context->getContextOption(FINAL_BLOCK_NUMBER, finalBlockNumberFromContext);
  if(finalBlockNumberFromContext > 0){
    m_finalBlockNumber = finalBlockNumberFromContext;
    m_isFinalBlockNumberDiscovered = true;
  }

  // Window size inheritance using context
  // contextに設定を外出しする設計になる。start()とstop()でm_currentWindowSizeが引き継げるはずだが。
  // @ToDo: contextに出すか否かについて整理する
  double windowSizeFromContext = -1;
  m_context->getContextOption(CURRENT_WINDOW_SIZE, windowSizeFromContext);
  if (windowSizeFromContext > 0){
    m_currentWindowSize = windowSizeFromContext;
    int maxWindowFromContext = 1;
    m_context->getContextOption(MAX_WINDOW_SIZE, maxWindowFromContext);
    // check flowcontrol protocol to determine first window size.
    // if flowcontrol is segmentFetcher (1), set window size to final block num.
    int flowControlFromContext = 1;
    m_context->getContextOption(FLOW_CONTROL, flowControlFromContext);
    if(flowControlFromContext == 1){
      if (m_isFinalBlockNumberDiscovered)
        m_currentWindowSize = m_finalBlockNumber;
      if (m_currentWindowSize > maxWindowFromContext)
        m_currentWindowSize = maxWindowFromContext;
    }
  }
  if (m_currentWindowSize > m_interestsInFlight){
    controlOutgoingInterests();
  }
  else{
    //send exactly 1 Interest to get the FinalBlockId
    sendInterest();
  }
  bool isAsync = false;
  m_context->getContextOption(ASYNC_MODE, isAsync);

  bool isContextRunning = false;
  m_context->getContextOption(RUNNING, isContextRunning);

  if (!isAsync && !isContextRunning) {
    m_context->setContextOption(RUNNING, true);
    m_face->processEvents();
  }
}

void
BBRDataRetrieval::sendInterest()
{
  Name prefix;
  m_context->getContextOption(PREFIX, prefix);

  Name suffix;
  m_context->getContextOption(SUFFIX, suffix);

  if (!suffix.empty()) {
    prefix.append(suffix);
  }

  prefix.appendSegment(m_segNumber);

  Interest interest(prefix);

  int interestLifetime = DEFAULT_INTEREST_LIFETIME_API;
  m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
  interest.setInterestLifetime(time::milliseconds(interestLifetime));

  SelectorHelper::applySelectors(interest, m_context);

  ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
  m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
  if (onInterestToLeaveContext != EMPTY_CALLBACK) {
    onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), interest);
  }

  // because user could stop the context in one of the prev callbacks
  //if (m_isRunning == false)
  //  return;

  m_interestsInFlight++;
  if(m_doLogging)
  {
    std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
    << " BBR::sendInterest, inflight, " << m_interestsInFlight << ", windowSize, " << m_currentWindowSize << ", name, " << interest.getName().toUri() << std::endl; 
  }
  m_interestRetransmissions[m_segNumber] = 0;
  m_interestTimepoints[m_segNumber] = time::steady_clock::now();
  m_expressedInterests[m_segNumber] = m_face->expressInterest(interest,
                                                              bind(&BBRDataRetrieval::onData, this, _1, _2),
                                                              bind(&BBRDataRetrieval::onNack, this, _1, _2),
                                                              bind(&BBRDataRetrieval::onTimeout, this, _1));
  m_segNumber++;
}

void
BBRDataRetrieval::stop()
{
  m_isRunning = false;
  m_interestsInFlight = 0;
  removeAllPendingInterests();
  removeAllScheduledInterests();
}

void
BBRDataRetrieval::getNetworkStatistics(double minRTT, double maxRTT, double currentWindow)
{
  minRTT = m_minRTT;
  maxRTT = m_maxRTT;
  currentWindow = m_currentWindowSize;
}

void
BBRDataRetrieval::onData(const ndn::Interest& interest, const ndn::Data& data)
{
  if (m_isRunning == false)
    return;

  m_interestsInFlight--;

  uint64_t segment = interest.getName().get(-1).toSegment();
  m_expressedInterests.erase(segment);
  // my RDR erased sentence of scheduledInterests.erase(), why???
  // 再送の解除のために消した方が良さそうだが、バグが出るのだろうか?
  m_scheduledInterests.erase(segment);

  if (m_interestTimepoints.find(segment) != m_interestTimepoints.end()) {
    time::steady_clock::duration duration = time::steady_clock::now() - m_interestTimepoints[segment];
    m_rttEstimator.addMeasurement(boost::chrono::duration_cast<boost::chrono::microseconds>(duration));
    RttEstimator::Duration rto = m_rttEstimator.computeRto();

    if(m_doLogging){
      std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
      << " BBR::onData::RTT = " << duration << ", name = " << data.getName().toUri() << std::endl; 
    }
    // Update min/max RTT
    double m = static_cast<double>(duration.count());
    if (m_minRTT == 0 || m_minRTT > m)
      m_minRTT = m;
    if (m_maxRTT < m)
      m_maxRTT = m; 

    int interestLifetime = DEFAULT_INTEREST_LIFETIME_API;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);

    // update lifetime only if user didn't specify prefered value
    if (interestLifetime == DEFAULT_INTEREST_LIFETIME_API) {
      boost::chrono::milliseconds lifetime = boost::chrono::duration_cast<boost::chrono::milliseconds>(rto);
      m_context->setContextOption(INTEREST_LIFETIME, (int)lifetime.count());
    }
  }

  ConsumerDataCallback onDataEnteredContext = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_ENTER_CNTX, onDataEnteredContext);
  if (onDataEnteredContext != EMPTY_CALLBACK) {
    onDataEnteredContext(*dynamic_cast<Consumer*>(m_context), data);
  }

  ConsumerInterestCallback onInterestSatisfied = EMPTY_CALLBACK;
  m_context->getContextOption(INTEREST_SATISFIED, onInterestSatisfied);
  if (onInterestSatisfied != EMPTY_CALLBACK) {
    onInterestSatisfied(*dynamic_cast<Consumer*>(m_context), const_cast<Interest&>(interest));
  }

  if (data.getContentType() == MANIFEST_DATA_TYPE) {
    onManifestData(interest, data);
  }
  else if (data.getContentType() == NACK_DATA_TYPE) {
    onNackData(interest, data);
  }
  else if (data.getContentType() == CONTENT_DATA_TYPE) {
    onContentData(interest, data);
  }
  
  if (m_isRunning){
    // SENDING NEXT INTERESTS: Control RWIN -> Schedule outgoing interests
    // 1. Control RWIN
    //    おそらくonContentData()で既に制御済み
    //    SegmentFetcherの場合、ここでfinalBlockIDと等しくしていた
    // 2. Schedule outgoing interests
    //    Current: pacing or not
    controlOutgoingInterests();
  }
}

void
BBRDataRetrieval::controlOutgoingInterests()
{
  // avoid duplicated pacing process
  if (m_nowPacing)
    return;
  int pacingInterval = 0;
  m_context->getContextOption(PACING_INTERVAL, pacingInterval);
  // Do pacing
  if (pacingInterval != 0){
    // totalのinflight数がRWINを超えないか確認.
    int totalInflight = m_interestsInFlight + m_scheduledInterests.size();
    if (m_currentWindowSize > totalInflight){
      // inflight availability
      int availability = m_currentWindowSize - totalInflight;
      if (m_isFinalBlockNumberDiscovered){
        if(m_doLogging){
          std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count() << ", " << time::steady_clock::now() - getStartTime()
          << " BBR::onData::availability: " << availability << ", totalInflight:" << totalInflight << ", scheduled: " << m_scheduledInterests.size()
          << ", m_segNumber: " << m_segNumber << ", m_finalBlockNumber: " << m_finalBlockNumber << ", m_currentWindowSize: " << m_currentWindowSize << std::endl;
        }
        if(m_finalBlockNumber >= m_segNumber + m_scheduledInterests.size()){
          // SendInterest until "m_segNumber == final"
          // m_segNumber + m_scheduled.size() = nextRequestSegmentNumber
          // # of interests required for finalBlock = finalBlockNumber - nextRequestSegmentNumber + 1
          if(availability >= m_finalBlockNumber - m_segNumber - m_scheduledInterests.size() + 1)
            paceInterests(m_finalBlockNumber - m_segNumber - m_scheduledInterests.size() + 1, time::milliseconds(pacingInterval));
          // not enough room for final number
          else
            paceInterests(availability, time::milliseconds(pacingInterval));
        }
      }
      else
        sendInterest();
    }
  }
  // No pacing
  else{
    while (m_currentWindowSize > m_interestsInFlight){
      if (m_isFinalBlockNumberDiscovered){
        if (m_segNumber <= m_finalBlockNumber)
          sendInterest();
        else
          break;
      }
      else
        sendInterest();
    }
  }
}

void
BBRDataRetrieval::paceInterests(int nInterests, time::milliseconds timeWindow)
{
  // 念のため
  if(m_nowPacing)
    return;
  m_nowPacing = true;
  if (nInterests <= 0)
  return;

  // constant interval for each interest
  time::nanoseconds interval = time::nanoseconds(timeWindow);
  // constant interval for set of interests
  // time::nanoseconds interval = time::nanoseconds(1000000 * timeWindow) / nInterests;

  if(m_doLogging){
    std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count()
    << ", " << time::steady_clock::now() - getStartTime()
    << " PACE INTEREST FOR " << nInterests << " Interests, Interval:" << interval << std::endl; 
  }

  int nextSegment = m_segNumber + m_scheduledInterests.size();

  for (int i = 1; i <= nInterests; i++) {
  if(m_doLogging){
    std::cout << "Schedule Interests (" << nextSegment << " -> " << nextSegment + nInterests 
    << ") in total " << i*interval << "ns" << std::endl;
  }
  // schedule next Interest
  m_scheduledInterests[nextSegment + i] = m_scheduler->scheduleEvent(i * interval, bind(&BBRDataRetrieval::sendInterest, this));
  }
}

void
BBRDataRetrieval::increaseWindow()
{
  double prevWindow = m_currentWindowSize;
  int maxWindowSize = 1;
  m_context->getContextOption(MAX_WINDOW_SIZE, maxWindowSize);
  int flowControlProtocol = 1;
  m_context->getContextOption(FLOW_CONTROL, flowControlProtocol);
  // [1] Segment fetcher, [2] AIMD, [3] VEGAS 
  switch(flowControlProtocol)
  {
    case 1:
      //SegmentFetcher();
      // In responce to the first Interest, try to transmit all Interests, except the first one as a next round 
      if (m_isFinalBlockNumberDiscovered)
        m_currentWindowSize = m_finalBlockNumber; 
      // if there are too many Interests to send, put an upper boundary on it.
      if (m_currentWindowSize > maxWindowSize) 
        m_currentWindowSize = maxWindowSize;
      break;

    case 2:
      // AIMD
      if (m_currentWindowSize < m_ssthresh){
        m_currentWindowSize += m_options.aiStep; //additive increase
      }
      // congestion avoidance
      else{
        m_currentWindowSize += m_options.aiStep / std::floor(m_currentWindowSize);
      }
      break;

    case 3:
      // VEGAS
      break;

    default:
      // default increase method
      if (m_currentWindowSize < maxWindowSize) // don't expand window above max level
      {
        m_currentWindowSize++;
      }    
      break;
  }
  // After increasing window size:
  m_context->setContextOption(CURRENT_WINDOW_SIZE, m_currentWindowSize);
  if(m_doLogging){
    std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count()
    << ", " << time::steady_clock::now() - getStartTime()
    << " cwnd changed: " << prevWindow << "->" << m_currentWindowSize << std::endl; 
  }
  // @ToDo
  //afterCwndChange(time::steady_clock::now() - getStartTime(), m_currentWindowSize);
}
void
BBRDataRetrieval::decreaseWindow()
{
    double prevWindow = m_currentWindowSize;
    int minWindowSize = -1;
    m_context->getContextOption(MIN_WINDOW_SIZE, minWindowSize);
    int flowControlProtocol = 1;
    m_context->getContextOption(FLOW_CONTROL, flowControlProtocol);
    // [1] Segment fetcher, [2] AIMD, [3] VEGAS 
    switch(flowControlProtocol)
    {
        case 1:
            if (m_currentWindowSize > minWindowSize) // don't shrink window below minimum level
            {
                m_currentWindowSize = m_currentWindowSize / 2; // cut in half
                if (m_currentWindowSize == 0)
                    m_currentWindowSize++;
            }
            break;
        case 2:
            // AIMD
            // please refer to RFC 5681, Section 3.1 for the rationale behind it
            m_ssthresh = std::max(2.0, m_currentWindowSize * m_options.mdCoef); // multiplicative decrease
            m_currentWindowSize = m_options.resetCwndToInit ? m_options.initCwnd : m_ssthresh;
            break;
        case 3:
            // VEGAS
            break;
        default:
            if (m_currentWindowSize > minWindowSize) // don't shrink window below minimum level
            {
                m_currentWindowSize = m_currentWindowSize / 2; // cut in half
                if (m_currentWindowSize == 0)
                    m_currentWindowSize++;
            }
            break;
    }
    // After decreasing window size:
    m_context->setContextOption(CURRENT_WINDOW_SIZE, m_currentWindowSize);
    if(m_doLogging){
        std::cout << ndn::time::toUnixTimestamp(time::system_clock::now()).count()
        << ", " << time::steady_clock::now() - getStartTime()
        << " cwnd changed: " << prevWindow << "->" << m_currentWindowSize << std::endl; 
    }
    //afterCwndChange(time::steady_clock::now() - getStartTime(), m_currentWindowSize);
}

void
BBRDataRetrieval::onManifestData(const ndn::Interest& interest, const ndn::Data& data)
{
  if (m_isRunning == false)
    return;

  //std::cout << "OnManifest" << std::endl;
  ConsumerDataVerificationCallback onDataToVerify = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_TO_VERIFY, onDataToVerify);

  bool isDataSecure = false;

  if (onDataToVerify == EMPTY_CALLBACK) {
    // perform integrity check if possible
    if (data.getSignature().getType() == tlv::DigestSha256) {
      isDataSecure = security::verifyDigest(data, DigestAlgorithm::SHA256);
    }
    else {
      isDataSecure = true; // TODO something more meaningful
    }
  }
  else {
    // run verification routine
    if (onDataToVerify(*dynamic_cast<Consumer*>(m_context), data)) {
      isDataSecure = true;
    }
  }

  if (isDataSecure) {
    checkFastRetransmissionConditions(interest);

    increaseWindow();
    shared_ptr<Manifest> manifest = make_shared<Manifest>(data);

    //std::cout << "MANIFEST CONTAINS " << manifest->size() << " names" << std::endl;

    m_verifiedManifests.insert(std::pair<uint64_t, shared_ptr<Manifest>>(data.getName().get(-1).toSegment(), manifest));

    m_receiveBuffer[manifest->getName().get(-1).toSegment()] = manifest;

    // TODO: names in manifest are in order, so we can exit the loop earlier
    for (auto it = m_unverifiedSegments.begin(); it != m_unverifiedSegments.end(); ++it) {
      if (!m_isRunning) {
        return;
      }

      // data segment is verified with manifest
      if (verifySegmentWithManifest(*manifest, *(it->second))) {
        if (!it->second->getFinalBlockId().empty()) {
          m_isFinalBlockNumberDiscovered = true;
          m_finalBlockNumber = it->second->getFinalBlockId().toSegment();
        }

        m_receiveBuffer[it->second->getName().get(-1).toSegment()] = it->second;
        reassemble();
      }
      else {
        // data segment failed verification with manifest
        // retransmit interest with implicit digest from the manifest
        retransmitInterestWithDigest(interest, data, *manifest);
      }
    }
  }
  else {
    // failed to verify manifest
    retransmitInterestWithExclude(interest, data);
  }
}

void
BBRDataRetrieval::retransmitFreshInterest(const ndn::Interest& interest)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);

  uint64_t segment = interest.getName().get(-1).toSegment();
  if (m_interestRetransmissions[segment] < maxRetransmissions) {
    if (m_isRunning) {
      Interest retxInterest(interest.getName()); // because we need new nonce
      int interestLifetime = DEFAULT_INTEREST_LIFETIME_API;
      m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
      retxInterest.setInterestLifetime(time::milliseconds(interestLifetime));

      SelectorHelper::applySelectors(retxInterest, m_context);

      retxInterest.setMustBeFresh(true); // to bypass cache

      // this is to inherit the exclusions from the nacked interest
      retxInterest.setExclude(interest.getExclude());

      ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
      m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);

      if (onInterestRetransmitted != EMPTY_CALLBACK) {
        onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), retxInterest);
      }

      ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
      m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
      if (onInterestToLeaveContext != EMPTY_CALLBACK) {
        onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), retxInterest);
      }

      // because user could stop the context in one of the prev callbacks
      if (m_isRunning == false)
        return;

      m_interestsInFlight++;
      m_interestRetransmissions[segment]++;
      m_expressedInterests[segment] = m_face->expressInterest(retxInterest,
                                                              bind(&BBRDataRetrieval::onData, this, _1, _2),
                                                              bind(&BBRDataRetrieval::onNack, this, _1, _2),
                                                              bind(&BBRDataRetrieval::onTimeout, this, _1));
    }
  }
  else {
    m_isRunning = false;
  }
}

bool
BBRDataRetrieval::retransmitInterestWithExclude(const ndn::Interest& interest, const Data& dataSegment)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);

  uint64_t segment = interest.getName().get(-1).toSegment();
  m_unverifiedSegments.erase(segment); // remove segment, because it is useless

  if (m_interestRetransmissions[segment] < maxRetransmissions) {
    Interest interestWithExlusion(interest.getName());
    int interestLifetime = DEFAULT_INTEREST_LIFETIME_API;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    interestWithExlusion.setInterestLifetime(time::milliseconds(interestLifetime));

    SelectorHelper::applySelectors(interestWithExlusion, m_context);

    int nMaxExcludedDigests = 0;
    m_context->getContextOption(MAX_EXCLUDED_DIGESTS, nMaxExcludedDigests);

    if (interest.getExclude().size() < nMaxExcludedDigests) {
      Exclude exclusion = interest.getExclude();
      exclusion.excludeOne(dataSegment.getFullName().get(-1));
      interestWithExlusion.setExclude(exclusion);
    }
    else {
      m_isRunning = false;
      return false;
    }

    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);

    if (onInterestRetransmitted != EMPTY_CALLBACK) {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), interestWithExlusion);
    }

    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK) {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), interestWithExlusion);
    }

    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return false;

    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segment]++;
    m_expressedInterests[segment] = m_face->expressInterest(interestWithExlusion,
                                                            bind(&BBRDataRetrieval::onData, this, _1, _2),
                                                            bind(&BBRDataRetrieval::onNack, this, _1, _2),
                                                            bind(&BBRDataRetrieval::onTimeout, this, _1));
  }
  else {
    m_isRunning = false;
    return false;
  }

  return true;
}

bool
BBRDataRetrieval::retransmitInterestWithDigest(const ndn::Interest& interest, const Data& dataSegment, const Manifest& manifestSegment)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);

  uint64_t segment = interest.getName().get(-1).toSegment();
  m_unverifiedSegments.erase(segment); // remove segment, because it is useless

  if (m_interestRetransmissions[segment] < maxRetransmissions) {
    name::Component implicitDigest = getDigestFromManifest(manifestSegment, dataSegment);
    if (implicitDigest.empty()) {
      m_isRunning = false;
      return false;
    }

    Name nameWithDigest(interest.getName());
    nameWithDigest.append(implicitDigest);

    Interest interestWithDigest(nameWithDigest);
    int interestLifetime = DEFAULT_INTEREST_LIFETIME_API;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    interestWithDigest.setInterestLifetime(time::milliseconds(interestLifetime));

    SelectorHelper::applySelectors(interestWithDigest, m_context);

    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);

    if (onInterestRetransmitted != EMPTY_CALLBACK) {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), interestWithDigest);
    }

    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK) {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), interestWithDigest);
    }

    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return false;

    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segment]++;
    m_expressedInterests[segment] = m_face->expressInterest(interestWithDigest,
                                                            bind(&BBRDataRetrieval::onData, this, _1, _2),
                                                            bind(&BBRDataRetrieval::onNack, this, _1, _2),
                                                            bind(&BBRDataRetrieval::onTimeout, this, _1));
  }
  else {
    m_isRunning = false;
    return false;
  }

  return true;
}

void
BBRDataRetrieval::onNackData(const ndn::Interest& interest, const ndn::Data& data)
{
  if (m_isRunning == false)
    return;

  if (m_isFinalBlockNumberDiscovered) {
    if (data.getName().at(-3).toSegment() > m_finalBlockNumber) {
      return;
    }
  }

  ConsumerDataVerificationCallback onDataToVerify = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_TO_VERIFY, onDataToVerify);

  bool isDataSecure = false;

  if (onDataToVerify == EMPTY_CALLBACK) {
    // perform integrity check if possible
    if (data.getSignature().getType() == tlv::DigestSha256) {
      isDataSecure = security::verifyDigest(data, DigestAlgorithm::SHA256);
    }
    else {
      isDataSecure = true; // TODO do something more meaningful
    }
  }
  else {
    // run verification routine
    if (onDataToVerify(*dynamic_cast<Consumer*>(m_context), data)) {
      isDataSecure = true;
    }
  }

  if (isDataSecure) {
    checkFastRetransmissionConditions(interest);

    decreaseWindow();

    shared_ptr<ApplicationNack> nack = make_shared<ApplicationNack>(data);

    ConsumerNackCallback onNack = EMPTY_CALLBACK;
    m_context->getContextOption(NACK_ENTER_CNTX, onNack);
    if (onNack != EMPTY_CALLBACK) {
      onNack(*dynamic_cast<Consumer*>(m_context), *nack);
    }

    switch (nack->getCode()) {
      case ApplicationNack::DATA_NOT_AVAILABLE: {
        m_isRunning = false;
        break;
      }

      case ApplicationNack::NONE: {
        // TODO: reduce window size ?
        break;
      }

      case ApplicationNack::PRODUCER_DELAY: {
        uint64_t segment = interest.getName().get(-1).toSegment();

        m_scheduledInterests[segment] = m_scheduler->scheduleEvent(time::milliseconds(nack->getDelay()),
                                                                   bind(&BBRDataRetrieval::retransmitFreshInterest, this, interest));

        break;
      }

      default:
        break;
    }
  }
  else // if NACK is not verified
  {
    retransmitInterestWithExclude(interest, data);
  }
}

void
BBRDataRetrieval::onContentData(const ndn::Interest& interest, const ndn::Data& data)
{
  ConsumerDataVerificationCallback onDataToVerify = EMPTY_CALLBACK;
  m_context->getContextOption(DATA_TO_VERIFY, onDataToVerify);

  bool isDataSecure = false;

  if (onDataToVerify == EMPTY_CALLBACK) {
    // perform integrity check if possible
    if (data.getSignature().getType() == tlv::DigestSha256) {
      isDataSecure = security::verifyDigest(data, DigestAlgorithm::SHA256);

      if (!isDataSecure) {
        retransmitInterestWithExclude(interest, data);
      }
    }
    else {
      isDataSecure = true;
    }
  }
  else {
    if (!data.getSignature().hasKeyLocator()) {
      retransmitInterestWithExclude(interest, data);
      return;
    }

    // if data segment points to inlined manifest
    if (referencesManifest(data)) {
      Name referencedManifestName = data.getSignature().getKeyLocator().getName();
      uint64_t manifestSegmentNumber = referencedManifestName.get(-1).toSegment();

      if (m_verifiedManifests.find(manifestSegmentNumber) == m_verifiedManifests.end()) {
        // save segment for some time, because manifest can be out of order
        //std::cout << "SAVING SEGMENT for MANIFEST" << std::endl;
        m_unverifiedSegments.insert(std::pair<uint64_t, shared_ptr<const Data>>(data.getName().get(-1).toSegment(), data.shared_from_this()));
      }
      else {
        //std::cout << "NEAREST M " << m_verifiedManifests[manifestSegmentNumber]->getName() << std::endl;
        isDataSecure = verifySegmentWithManifest(*(m_verifiedManifests[manifestSegmentNumber]), data);

        if (!isDataSecure) {
          //std::cout << "Retx Digest" << std::endl;
          retransmitInterestWithDigest(interest, data, *m_verifiedManifests.find(manifestSegmentNumber)->second);
        }
      }
    }
    else { // data segment points to the key
      // runs verification routine
      if (onDataToVerify(*dynamic_cast<Consumer*>(m_context), data) == true) {
        isDataSecure = true;
      }
      else {
        retransmitInterestWithExclude(interest, data);
      }
    }
  }

  if (isDataSecure) {
    checkFastRetransmissionConditions(interest);    
    if (!data.getFinalBlockId().empty()) {
      m_isFinalBlockNumberDiscovered = true;
      m_finalBlockNumber = data.getFinalBlockId().toSegment();
    }
    increaseWindow();

    m_receiveBuffer[data.getName().get(-1).toSegment()] = data.shared_from_this();
    reassemble();
  }
}

bool
BBRDataRetrieval::referencesManifest(const ndn::Data& data)
{
  Name keyLocatorPrefix = data.getSignature().getKeyLocator().getName().getPrefix(-1);
  Name dataPrefix = data.getName().getPrefix(-1);

  if (keyLocatorPrefix.equals(dataPrefix)) {
    return true;
  }

  return false;
}

void
BBRDataRetrieval::onNack(const Interest& interest, const lp::Nack& nack)
{
  onTimeout(interest); // TODO: make proper handling of NACK, this can result in kind of a loop
}

void
BBRDataRetrieval::onTimeout(const ndn::Interest& interest)
{
  if (m_isRunning == false)
    return;

  m_interestsInFlight--;

  ConsumerInterestCallback onInterestExpired = EMPTY_CALLBACK;
  m_context->getContextOption(INTEREST_EXPIRED, onInterestExpired);
  if (onInterestExpired != EMPTY_CALLBACK) {
    onInterestExpired(*dynamic_cast<Consumer*>(m_context), const_cast<Interest&>(interest));
  }

  uint64_t segment = interest.getName().get(-1).toSegment();
  m_expressedInterests.erase(segment);
  m_scheduledInterests.erase(segment);

  if (m_isFinalBlockNumberDiscovered) {
    if (interest.getName().get(-1).toSegment() > m_finalBlockNumber)
      return;
  }

  decreaseWindow();

  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);

  if (m_interestRetransmissions[segment] < maxRetransmissions) {
    Interest retxInterest(interest.getName()); // because we need new nonce
    int interestLifetime = DEFAULT_INTEREST_LIFETIME_API;
    m_context->getContextOption(INTEREST_LIFETIME, interestLifetime);
    retxInterest.setInterestLifetime(time::milliseconds(interestLifetime));

    SelectorHelper::applySelectors(retxInterest, m_context);

    // this is to inherit the exclusions from the timed out interest
    retxInterest.setExclude(interest.getExclude());

    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);

    if (onInterestRetransmitted != EMPTY_CALLBACK) {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }

    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK) {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }

    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return;

    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segment]++;
    m_expressedInterests[segment] = m_face->expressInterest(retxInterest,
                                                            bind(&BBRDataRetrieval::onData, this, _1, _2),
                                                            bind(&BBRDataRetrieval::onNack, this, _1, _2),
                                                            bind(&BBRDataRetrieval::onTimeout, this, _1));
  }
  else {
    m_isRunning = false;
    reassemble(); // to pass up all content we have so far
  }
}

void
BBRDataRetrieval::copyContent(const Data& data)
{
  const Block content = data.getContent();
  m_contentBuffer.insert(m_contentBuffer.end(), &content.value()[0], &content.value()[content.value_size()]);

  if ((data.getName().at(-1).toSegment() == m_finalBlockNumber) || (!m_isRunning)) {
    removeAllPendingInterests();
    removeAllScheduledInterests();

    // return content to the user
    ConsumerContentCallback onPayload = EMPTY_CALLBACK;
    m_context->getContextOption(CONTENT_RETRIEVED, onPayload);
    if (onPayload != EMPTY_CALLBACK) {
      onPayload(*dynamic_cast<Consumer*>(m_context), m_contentBuffer.data(), m_contentBuffer.size());
    }

    // @ToDo: 相当怪しい
    //reduce window size to prevent its speculative growth in case when consume() is called in loop
    double currentWindowSize = -1;
    m_context->getContextOption(CURRENT_WINDOW_SIZE, currentWindowSize);
    if (currentWindowSize > m_finalBlockNumber) {
      m_context->setContextOption(CURRENT_WINDOW_SIZE, (int)(m_finalBlockNumber));
    }

    m_isRunning = false;
  }
}

void
BBRDataRetrieval::reassemble()
{
  auto head = m_receiveBuffer.find(m_lastReassembledSegment);
  while (head != m_receiveBuffer.end()) {
    // do not copy from manifests
    if (head->second->getContentType() == CONTENT_DATA_TYPE) {
      copyContent(*(head->second));
    }

    m_receiveBuffer.erase(head);
    m_lastReassembledSegment++;
    head = m_receiveBuffer.find(m_lastReassembledSegment);
  }
}

bool
BBRDataRetrieval::verifySegmentWithManifest(const Manifest& manifestSegment, const Data& dataSegment)
{
  //std::cout << "Verify Segment With MAnifest" << std::endl;
  bool result = false;

  for (std::list<Name>::const_iterator it = manifestSegment.catalogueBegin(); it != manifestSegment.catalogueEnd(); ++it) {
    if (it->get(-2) == dataSegment.getName().get(-1)) // if segment numbers match
    {
      if (dataSegment.getFullName().get(-1) == it->get(-1)) {
        result = true;
        break;
      }
      else {
        break;
      }
    }
  }

  //if (!result)
  //  std::cout << "Segment failed verification by manifest" << result<< std::endl;

  return result;
}

name::Component
BBRDataRetrieval::getDigestFromManifest(const Manifest& manifestSegment, const Data& dataSegment)
{
  name::Component result;

  for (std::list<Name>::const_iterator it = manifestSegment.catalogueBegin(); it != manifestSegment.catalogueEnd(); ++it) {
    if (it->get(-2) == dataSegment.getName().get(-1)) // if segment numbers match
    {
      result = it->get(-1);
      return result;
    }
  }

  return result;
}

void
BBRDataRetrieval::checkFastRetransmissionConditions(const ndn::Interest& interest)
{
  uint64_t segNumber = interest.getName().get(-1).toSegment();
  m_receivedSegments[segNumber] = true;
  m_fastRetxSegments.erase(segNumber);

  uint64_t possiblyLostSegment = 0;
  uint64_t highestReceivedSegment = m_receivedSegments.rbegin()->first;

  for (uint64_t i = 0; i <= highestReceivedSegment; i++) {
    if (m_receivedSegments.find(i) == m_receivedSegments.end()) // segment is not received yet
    {
      // segment has not been fast retransmitted yet
      if (m_fastRetxSegments.find(i) == m_fastRetxSegments.end()) {
        possiblyLostSegment = i;
        uint8_t nOutOfOrderSegments = 0;
        for (uint64_t j = i; j <= highestReceivedSegment; j++) {
          if (m_receivedSegments.find(j) != m_receivedSegments.end()) {
            nOutOfOrderSegments++;
            if (nOutOfOrderSegments == DEFAULT_FAST_RETX_CONDITION) {
              m_fastRetxSegments[possiblyLostSegment] = true;
              fastRetransmit(interest, possiblyLostSegment);
            }
          }
        }
      }
    }
  }
}

void
BBRDataRetrieval::fastRetransmit(const ndn::Interest& interest, uint64_t segNumber)
{
  int maxRetransmissions;
  m_context->getContextOption(INTEREST_RETX, maxRetransmissions);

  if (m_interestRetransmissions[segNumber] < maxRetransmissions) {
    Name name = interest.getName().getPrefix(-1);
    name.appendSegment(segNumber);

    Interest retxInterest(name);
    SelectorHelper::applySelectors(retxInterest, m_context);

    // this is to inherit the exclusions from the lost interest
    retxInterest.setExclude(interest.getExclude());

    ConsumerInterestCallback onInterestRetransmitted = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_RETRANSMIT, onInterestRetransmitted);

    if (onInterestRetransmitted != EMPTY_CALLBACK) {
      onInterestRetransmitted(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }

    ConsumerInterestCallback onInterestToLeaveContext = EMPTY_CALLBACK;
    m_context->getContextOption(INTEREST_LEAVE_CNTX, onInterestToLeaveContext);
    if (onInterestToLeaveContext != EMPTY_CALLBACK) {
      onInterestToLeaveContext(*dynamic_cast<Consumer*>(m_context), retxInterest);
    }

    // because user could stop the context in one of the prev callbacks
    if (m_isRunning == false)
      return;

    //retransmit
    m_interestsInFlight++;
    m_interestRetransmissions[segNumber]++;
    //std::cout << "fast retx" << std::endl;
    m_expressedInterests[segNumber] = m_face->expressInterest(retxInterest,
                                                              bind(&BBRDataRetrieval::onData, this, _1, _2),
                                                              bind(&BBRDataRetrieval::onNack, this, _1, _2),
                                                              bind(&BBRDataRetrieval::onTimeout, this, _1));
  }
}

void
BBRDataRetrieval::removeAllPendingInterests()
{
  bool isAsync = false;
  m_context->getContextOption(ASYNC_MODE, isAsync);

  if (!isAsync) {
    // This won't work ---> m_face->getIoService().stop();
    m_face->removeAllPendingInterests(); // faster, but destroys everything
  }
  else // slower, but destroys only necessary Interests
  {
    for (std::unordered_map<uint64_t, const PendingInterestId*>::iterator it = m_expressedInterests.begin(); it != m_expressedInterests.end();
         ++it) {
      m_face->removePendingInterest(it->second);
    }
  }

  m_expressedInterests.clear();
}

void
BBRDataRetrieval::removeAllScheduledInterests()
{
  for (std::unordered_map<uint64_t, EventId>::iterator it = m_scheduledInterests.begin(); it != m_scheduledInterests.end(); ++it) {
    m_scheduler->cancelEvent(it->second);
  }

  m_scheduledInterests.clear();
}

} //namespace ndn
