#ifndef WINDOWSBUNDLESINKEVALUATOR_H_
#define WINDOWSBUNDLESINKEVALUATOR_H_

#include "core/SingleInputTransformEvaluator.h"
#include "Sink/Sink.h"
#include "Values.h"
#include <map>

extern std::map<Window, ptime, Window> window_keeper;


/* InputT: the element type of the record bundle */
template <typename InputT>
class WindowsBundleSinkEvaluator
    : public SingleInputTransformEvaluator<WindowsBundleSink<InputT>,
//      WindowsKeyedBundle<InputT>, WindowsBundle<InputT>> {
                                           WindowsBundle<InputT>, WindowsBundle<InputT>> {   // sufficient for wc & topK?

	using TransformT = WindowsBundleSink<InputT>;
	using InputBundleT = WindowsBundle<InputT>;
//	using InputBundleT = WindowsKeyedBundle<InputT>;
	//using InputBundleT = WindowsKeyedBundle<KVPair>;
	using OutputBundleT = WindowsBundle<InputT>;

public:

	WindowsBundleSinkEvaluator(int node)
        : SingleInputTransformEvaluator<TransformT, InputBundleT, OutputBundleT>(node) { }

    bool evaluateSingleInput (TransformT* trans,
                              shared_ptr<InputBundleT> input_bundle,
                              shared_ptr<OutputBundleT> output_bundle) override {

        //XXX TransformT::printBundle(*input_bundle);
        // TransformT::report_progress(* input_bundle);
        // TransformT::printBundle(*input_bundle);

        int size = 0;
        for (auto && win_frag: input_bundle->vals) {
            auto && win = win_frag.first;
            auto && pfrag = win_frag.second;

            for (auto &kv : pfrag->vals) {
                auto k = kv.data.first;
                auto v = kv.data.second;
                // EE("win.start: %s, k: %lu, v: %lu", to_simplest_string(win.window_start()).c_str(), k, v);

            }
            size += pfrag->vals.size();
        }

        if (size > 0) {
            trans->record_counter_.fetch_add(size, std::memory_order_relaxed);
        }

        return false; /* no output bundle */
    }
};

#endif /* WINDOWSBUNDLESINKEVALUATOR_H_ */
